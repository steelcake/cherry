from dataclasses import dataclass
from typing import Optional, Dict
import polars as pl

@dataclass
class Data:
    """Container for blockchain data"""
    blocks: Optional[Dict[str, pl.DataFrame]] = None
    transactions: Optional[Dict[str, pl.DataFrame]] = None
    events: Optional[Dict[str, pl.DataFrame]] = None 