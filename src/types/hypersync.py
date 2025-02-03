from dataclasses import dataclass
from typing import Dict, List, Optional
import polars as pl
from hypersync import HypersyncClient

@dataclass
class StreamParams:
    """Parameters for streaming event data"""
    client: HypersyncClient
    event_name: str
    signature: str
    from_block: int
    items_per_section: int
    to_block: Optional[int] = None
    contract_addr_list: Optional[List[pl.Series]] = None
    column_mapping: Optional[Dict[str, pl.DataType]] = None
    output_dir: Optional[str] = None
