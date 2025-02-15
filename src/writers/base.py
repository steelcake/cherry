from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
import polars as pl
import logging
import pyarrow as pa

logger = logging.getLogger(__name__)

class DataWriter(ABC):
    """Base class for data writers"""

    @abstractmethod
    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to target storage"""
        pass

