import asyncio
from functools import wraps


def async_retry(max_retries: int = 3, delay: float = 1.0):
    """Decorator for async retry logic"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay * (2 ** attempt))  # Exponential backoff
                    continue
            raise last_exception
        return wrapper
    return decorator