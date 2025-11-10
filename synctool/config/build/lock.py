"""
Build lock manager for atomic operations.
"""
import asyncio
import time
from pathlib import Path
import logging
import os
import fcntl


class BuildLockManager:
    """
    Manages build locks to prevent concurrent build operations.
    Uses file-based locking for simplicity.
    """
    
    def __init__(self, built_config_dir: Path, timeout: int = 30):
        """
        Initialize build lock manager.
        
        Args:
            built_config_dir: Directory where built configs are stored
            timeout: Lock acquisition timeout in seconds
        """
        self.built_config_dir = Path(built_config_dir)
        self.timeout = timeout
        self.lock_file_path = self.built_config_dir / ".build.lock"
        self.lock_file = None
        self.logger = logging.getLogger(__name__)
        
        # Ensure directory exists
        self.built_config_dir.mkdir(parents=True, exist_ok=True)
    
    async def __aenter__(self):
        """Acquire lock (async context manager)"""
        await self.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Release lock (async context manager)"""
        await self.release()
        return False
    
    async def acquire(self):
        """
        Acquire build lock with timeout.
        
        Raises:
            TimeoutError: If lock cannot be acquired within timeout
        """
        start_time = time.time()
        
        self.logger.debug(f"Attempting to acquire build lock: {self.lock_file_path}")
        
        while True:
            try:
                # Open lock file (create if doesn't exist)
                self.lock_file = open(self.lock_file_path, 'w')
                
                # Try to acquire exclusive lock (non-blocking)
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # Write PID to lock file
                self.lock_file.write(f"{os.getpid()}\n")
                self.lock_file.flush()
                
                self.logger.info("Build lock acquired")
                return
                
            except BlockingIOError:
                # Lock is held by another process
                elapsed = time.time() - start_time
                
                if elapsed >= self.timeout:
                    self.logger.error(
                        f"Failed to acquire build lock after {self.timeout}s timeout"
                    )
                    if self.lock_file:
                        self.lock_file.close()
                        self.lock_file = None
                    raise TimeoutError(
                        f"Could not acquire build lock within {self.timeout}s. "
                        "Another build/deploy operation may be in progress."
                    )
                
                # Wait a bit and retry
                self.logger.debug(
                    f"Build lock held by another process, retrying... "
                    f"({elapsed:.1f}s / {self.timeout}s)"
                )
                await asyncio.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"Failed to acquire build lock: {e}")
                if self.lock_file:
                    self.lock_file.close()
                    self.lock_file = None
                raise
    
    async def release(self):
        """Release build lock"""
        if not self.lock_file:
            return
        
        try:
            # Release lock
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
            self.lock_file.close()
            self.lock_file = None
            
            # Remove lock file
            if self.lock_file_path.exists():
                self.lock_file_path.unlink()
            
            self.logger.info("Build lock released")
            
        except Exception as e:
            self.logger.error(f"Failed to release build lock: {e}")
    
    def is_locked(self) -> bool:
        """
        Check if build is currently locked (non-blocking check).
        
        Returns:
            True if locked
        """
        if not self.lock_file_path.exists():
            return False
        
        try:
            # Try to open and lock
            test_file = open(self.lock_file_path, 'r')
            fcntl.flock(test_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # We got the lock, so it wasn't locked
            fcntl.flock(test_file.fileno(), fcntl.LOCK_UN)
            test_file.close()
            return False
            
        except BlockingIOError:
            # Lock is held
            return True
            
        except Exception:
            # Assume locked on error
            return True

