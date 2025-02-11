from abc import ABC, abstractmethod
from typing import AsyncGenerator
import logging
from src.types.data import Data

logger = logging.getLogger(__name__)

class DataIngester(ABC):
    """Abstract base class for data ingesters"""
    
    @abstractmethod
    async def process_data(self) -> AsyncGenerator[Data, None]:
        """Process and yield data"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources"""
        pass

    @property
    @abstractmethod
    def current_block(self) -> int:
        """Get current block number"""
        pass

    async def __aiter__(self) -> AsyncGenerator[Data, None]:
        """Make ingester iterable"""
        try:
            async for data in self.process_data():
                if not data.is_empty():
                    yield data
        finally:
            await self.close()
