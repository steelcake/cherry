from dataclasses import dataclass
from typing import Dict, Optional
import polars as pl

@dataclass
class Data:
    """Core data structure for blockchain data"""
    events: Optional[Dict[str, pl.DataFrame]] = None
    blocks: Optional[Dict[str, pl.DataFrame]] = None
    transactions: Optional[Dict[str, pl.DataFrame]] = None 