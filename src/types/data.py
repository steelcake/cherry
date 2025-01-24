from dataclasses import dataclass
from typing import Dict, Optional
import polars as pl

@dataclass
class Data:
    """Container for blockchain data"""
    events: Optional[Dict[str, pl.DataFrame]] = None
    blocks: Optional[Dict[str, pl.DataFrame]] = None
    transactions: Optional[Dict[str, pl.DataFrame]] = None

    def __bool__(self) -> bool:
        """Return True if any data is present"""
        return any([self.events, self.blocks, self.transactions])