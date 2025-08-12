from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class BaseCache(ABC):
    @abstractmethod
    async def get(self, key: Any) -> Optional[Dict]:
        pass

    @abstractmethod
    async def set(self, key: Any, value: Dict, ttl: Optional[int] = None):
        pass

    async def connect(self, config: dict):
        # No external connection needed
        pass
    
    async def disconnect(self):
        # No resource cleanup needed
        pass