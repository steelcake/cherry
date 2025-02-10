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

    @staticmethod
    def combine_blocks(data: Dict[str, pa.RecordBatch]) -> Optional[pa.Table]:
        """Combine and deduplicate block tables"""
        blocks_tables = [name for name in data if name.startswith('blocks_')]
        if not blocks_tables:
            return None
        
        # Combine all blocks into one table
        blocks_data = pa.concat_tables([
            pa.Table.from_batches([data[name]]) 
            for name in blocks_tables
        ])
        
        # Convert to pandas for easier manipulation
        blocks_df = blocks_data.to_pandas()
        
        # Get block range from events
        event_tables = [name for name in data if name.endswith('_events')]
        if event_tables:
            # Find min/max blocks across all event tables
            event_blocks = pd.concat([
                data[table].to_pandas()['block_number'] 
                for table in event_tables
            ])
            min_block = event_blocks.min()
            max_block = event_blocks.max()
            
            # Filter blocks to event range
            blocks_df = blocks_df[
                (blocks_df['block_number'] >= min_block) & 
                (blocks_df['block_number'] <= max_block)
            ]
            logger.info(f"Filtered blocks to event range {min_block} to {max_block}")
        
        # Sort and deduplicate
        blocks_df = (blocks_df
                    .sort_values('block_number')
                    .drop_duplicates(subset=['block_number'], keep='last'))
        
        logger.info(f"Combined {len(blocks_df)} unique blocks from "
                   f"{blocks_df['block_number'].min()} to {blocks_df['block_number'].max()}")
        
        return pa.Table.from_pandas(blocks_df)

    @staticmethod
    def log_event_stats(data: Dict[str, pa.RecordBatch]) -> None:
        """Log statistics for event tables"""
        for table_name in [t for t in data if t.endswith('_events')]:
            df = data[table_name].to_pandas()
            logger.info(f"{table_name}: {len(df)} events, blocks "
                       f"{df['block_number'].min()} to {df['block_number'].max()}")

    @abstractmethod
    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to target storage"""
        pass
