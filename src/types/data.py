import logging
from typing import Dict, Optional, Tuple
import pyarrow as pa

logger = logging.getLogger(__name__)

class Data:
    """Generic container for multiple Arrow RecordBatch collections"""
    
    def __init__(self, **kwargs: Dict[str, pa.RecordBatch]):
        self.collections = kwargs
        logger.info(f"Created Data with collections: {list(self.collections.keys())}")

    @property
    def blocks(self):
        """Get blocks collection"""
        return self.collections.get('blocks', {}).get('blocks', None)

    @property
    def events(self):
        """Get events collection"""
        return self.collections.get('events', {})

    @property
    def transactions(self):
        """Get transactions collection"""
        return self.collections.get('transactions', {}).get('transactions', None)

    @property
    def traces(self):
        """Get traces collection"""
        return self.collections.get('traces', {}).get('traces', None)

    def is_empty(self) -> bool:
        """Check if all collections are empty"""
        return all(
            batch is None or (isinstance(batch, dict) and all(
                inner_batch is None or inner_batch.num_rows == 0
                for inner_batch in batch.values()
            )) or (hasattr(batch, 'num_rows') and batch.num_rows == 0)
            for collection in self.collections.values()
            for batch in collection.values()
        )

    def __str__(self) -> str:
        """Return human-readable string representation"""
        parts = []
        for name, collection in self.collections.items():
            total_rows = sum(
                batch.num_rows for batch in collection.values()
                if batch is not None and batch.num_rows > 0
            )
            parts.append(f"{len(collection)} {name} batches ({total_rows} total rows)")
            
        return f"Data({', '.join(parts) if parts else 'empty'})"

    def __getattr__(self, name):
        """Allow access to collections as attributes"""
        if name in self.collections:
            return self.collections[name]
        raise AttributeError(f"'Data' object has no attribute '{name}'")

    def __getitem__(self, name):
        """Allow dictionary-style access to collections"""
        return self.collections[name]

    def __contains__(self, name):
        """Check if collection exists"""
        return name in self.collections

    def get_latest_block(self) -> int:
        """Get the latest block number across all collections"""
        max_block = 0
        
        for collection in self.collections.values():
            for batch in collection.values():
                if isinstance(batch, dict):
                    # Handle nested dictionary structure
                    for inner_batch in batch.values():
                        if inner_batch and inner_batch.num_rows > 0:
                            # Look for block number column in the schema
                            block_col = next((col for col in ['blockNumber', 'block_number', 'number'] if col in inner_batch.schema.names), None)
                            if block_col:
                                # Get the maximum block number in this batch
                                batch_max = pa.compute.max(inner_batch.column(block_col)).as_py()
                                max_block = max(max_block, batch_max)
                elif batch and batch.num_rows > 0:
                    # Handle direct RecordBatch
                    # Look for block number column in the schema
                    block_col = next((col for col in ['blockNumber', 'block_number', 'number'] if col in batch.schema.names), None)
                    if block_col:
                        # Get the maximum block number in this batch
                        batch_max = pa.compute.max(batch.column(block_col)).as_py()
                        max_block = max(max_block, batch_max)
        
        logger.info(f"Latest block: {max_block}")
        return max_block

    def items(self):
        """Return items from all collections"""
        return self.collections.items()

    def keys(self):
        """Return keys from all collections"""
        return self.collections.keys()

    def __len__(self):
        """Return total number of collections"""
        return len(self.collections)