from dataclasses import dataclass
from typing import Dict, Optional, Iterator, Tuple
import polars as pl
from datetime import datetime

@dataclass
class Data:
    """Container for blockchain data"""
    events: Optional[Dict[str, pl.DataFrame]] = None
    blocks: Optional[Dict[str, pl.DataFrame]] = None
    transactions: Optional[Dict[str, pl.DataFrame]] = None

    @property
    def block_range(self) -> Optional[Tuple[int, int]]:
        """Return the min and max block numbers across all data"""
        min_block = float('inf')
        max_block = float('-inf')
        
        # Check blocks
        if self.blocks:
            for df in self.blocks.values():
                if 'block_number' in df.columns and df.height > 0:
                    min_block = min(min_block, df['block_number'].min())
                    max_block = max(max_block, df['block_number'].max())
        
        # Check events
        if self.events:
            for df in self.events.values():
                if 'block_number' in df.columns and df.height > 0:
                    min_block = min(min_block, df['block_number'].min())
                    max_block = max(max_block, df['block_number'].max())
        
        return (int(min_block), int(max_block)) if min_block != float('inf') else None
    
    @property
    def total_events(self) -> int:
        """Return total number of events across all event types"""
        return sum(df.height for df in self.events.values()) if self.events else 0
    
    @property
    def total_blocks(self) -> int:
        """Return total number of unique blocks"""
        if not self.blocks:
            return 0
        unique_blocks = set()
        for df in self.blocks.values():
            if 'block_number' in df.columns:
                unique_blocks.update(df['block_number'].unique().to_list())
        return len(unique_blocks)
    
    def iter_events(self) -> Iterator[Tuple[str, pl.DataFrame]]:
        """Iterate over events in a consistent order"""
        if self.events:
            for event_name in sorted(self.events.keys()):
                yield event_name, self.events[event_name]

    def __bool__(self) -> bool:
        """Return True if any data is present"""
        return any([self.events, self.blocks, self.transactions])

    def __str__(self) -> str:
        """Return human-readable string representation"""
        parts = []
        if self.events:
            parts.append(f"{len(self.events)} event types ({self.total_events} total events)")
        if self.blocks:
            parts.append(f"{self.total_blocks} unique blocks")
        if self.transactions:
            parts.append(f"{sum(df.height for df in self.transactions.values())} transactions")
        
        block_range = self.block_range
        if block_range:
            parts.append(f"block range: {block_range[0]} to {block_range[1]}")
            
        return f"Data({', '.join(parts) if parts else 'empty'})"