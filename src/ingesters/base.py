from abc import ABC, abstractmethod
from typing import Dict, AsyncGenerator
import logging
from src.types.data import Data
from src.writers.base import DataWriter
from src.utils.logging_setup import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

class DataIngester(ABC):
    """Abstract base class for data ingesters"""
    @abstractmethod
    async def get_data(self, from_block: int) -> AsyncGenerator[Data, None]:
        """Stream data from the specified block"""
        pass 

    @abstractmethod
    async def initialize_writers(self, writers: Dict[str, DataWriter]) -> None:
        """Initialize data writers"""
        pass

    @property
    @abstractmethod
    def current_block(self) -> int:
        """Get current block number"""
        pass
