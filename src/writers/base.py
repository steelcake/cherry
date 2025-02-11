from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, Callable
import polars as pl
import logging
from src.types.data import Data
from src.schemas.blockchain_schemas import BLOCKS, EVENTS
from src.schemas.base import SchemaConverter
import pyarrow as pa
from src.config.parser import Output
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

class DataWriter(ABC):
    """Base class for data writers"""

    def __init__(self):
        self.combine_blocks: Optional[Callable] = None

    def set_combine_blocks(self, combine_blocks_fn: Callable) -> None:
        """Set the combine_blocks function"""
        self.combine_blocks = combine_blocks_fn

    @abstractmethod
    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to target storage"""
        pass
