from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
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

    @abstractmethod
    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to target storage"""
        pass
