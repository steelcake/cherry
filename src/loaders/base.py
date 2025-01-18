from abc import ABC, abstractmethod
from typing import Any
import logging

logger = logging.getLogger(__name__)

class DataLoader(ABC):
    """Base class for data loaders"""
    
    @abstractmethod
    async def write_data(self, data: Any, **kwargs) -> None:
        """Write data to target"""
        pass 