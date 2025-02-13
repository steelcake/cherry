from abc import ABC, abstractmethod
from typing import Dict
import pyarrow as pa
from src.types.data import Data

class DataWriter(ABC):
    """Abstract base class for data writers"""

    @abstractmethod
    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write data in RecordBatch format"""
        pass

    @abstractmethod
    async def write(self, data: Data) -> None:
        """Write data in Data format"""
        pass
