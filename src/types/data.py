from dataclasses import dataclass
from typing import Dict, Optional
import pandas as pd

@dataclass
class Data:
    """Container for blockchain data"""
    events: Optional[Dict[str, pd.DataFrame]] = None
    blocks: Optional[Dict[str, pd.DataFrame]] = None
    transactions: Optional[Dict[str, pd.DataFrame]] = None

    def __bool__(self) -> bool:
        """Return True if there is any data"""
        return bool(self.events or self.blocks or self.transactions)

    def get_latest_block(self) -> int:
        """Get the latest block number from any data"""
        latest_block = 0
        
        if self.events:
            for df in self.events.values():
                if 'block_number' in df.columns:
                    latest_block = max(latest_block, df['block_number'].max())
                    
        if self.blocks:
            for df in self.blocks.values():
                if 'number' in df.columns:
                    latest_block = max(latest_block, df['number'].max())
                    
        if self.transactions:
            for df in self.transactions.values():
                if 'block_number' in df.columns:
                    latest_block = max(latest_block, df['block_number'].max())
                    
        return latest_block

    def get_block_range(self) -> tuple[int, int]:
        """Get the block range (min, max) across all data"""
        min_block = float('inf')
        max_block = 0
        
        for data_dict in [self.events, self.blocks, self.transactions]:
            if data_dict:
                for df in data_dict.values():
                    block_col = 'block_number' if 'block_number' in df.columns else 'number'
                    if block_col in df.columns:
                        min_block = min(min_block, df[block_col].min())
                        max_block = max(max_block, df[block_col].max())
        
        return (int(min_block), int(max_block)) if max_block > 0 else (0, 0)

    def is_empty(self) -> bool:
        """Check if all data is empty"""
        if not self:
            return True
            
        if self.events and any(not df.empty for df in self.events.values()):
            return False
            
        if self.blocks and any(not df.empty for df in self.blocks.values()):
            return False
            
        if self.transactions and any(not df.empty for df in self.transactions.values()):
            return False
            
        return True

    def __str__(self) -> str:
        """Return human-readable string representation"""
        parts = []
        if self.events:
            parts.append(f"{len(self.events)} event types ({sum(df.height for df in self.events.values()) if self.events else 0} total events)")
        if self.blocks:
            parts.append(f"{len(self.blocks)} unique blocks")
        if self.transactions:
            parts.append(f"{sum(df.height for df in self.transactions.values())} transactions")
        
        block_range = self.get_block_range()
        if block_range != (0, 0):
            parts.append(f"block range: {block_range[0]} to {block_range[1]}")
            
        return f"Data({', '.join(parts) if parts else 'empty'})"