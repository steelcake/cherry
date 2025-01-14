import hypersync
from hypersync import BlockField, ColumnMapping, HexOutput, HypersyncClient, LogField, Query, ClientConfig, LogSelection, StreamConfig, FieldSelection
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, cast, Dict
import polars as pl, logging
from src.utils.logging_setup import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

@dataclass
class Data:
    """Container for blockchain data"""
    blocks: pl.DataFrame
    transactions: Optional[pl.DataFrame]
    events: Dict[str, pl.DataFrame]

@dataclass
class GetParquetParams:
    client: HypersyncClient
    column_mapping: Dict[str, hypersync.DataType]
    event_name: str
    signature: str
    contract_addr_list: Optional[List[pl.Series]]
    from_block: int
    to_block: Optional[int]
    items_per_section: int

class DataIngester(ABC):
    """Abstract base class for data ingesters"""
    @abstractmethod
    async def get_data(self, from_block: int, to_block: int) -> Data:
        """Fetch data for the specified block range"""
        pass 
