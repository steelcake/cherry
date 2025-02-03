from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import polars as pl
import logging
from src.types.data import Data
from src.schemas.blockchain_schemas import BLOCKS, EVENTS
from src.schemas.base import SchemaConverter

logger = logging.getLogger(__name__)

class DataWriter(ABC):
    """Base class for data writers"""
    

    def __init__(self):
        self.events_schema = SchemaConverter.to_polars(EVENTS)
        self.blocks_schema = SchemaConverter.to_polars(BLOCKS)
    
    def prepare_data(self, data: Data) -> tuple[Optional[pl.DataFrame], Dict[str, pl.DataFrame]]:
        """Prepare and validate data for writing"""
        try:
            # Prepare blocks data
            blocks_df = None
            if data.blocks and isinstance(data.blocks, dict):
                all_blocks = []
                for event_name, blocks_df in data.blocks.items():
                    if blocks_df.height > 0:
                        logger.debug(f"Processing {blocks_df.height} blocks from {event_name}")
                        # Apply schema validation and casting
                        blocks_df = blocks_df.cast(self.blocks_schema)
                        all_blocks.append(blocks_df)
                
                if all_blocks:
                    blocks_df = pl.concat(all_blocks).unique(subset=["block_number"]).sort("block_number")
                    logger.info(f"Prepared {blocks_df.height} unique blocks")
                    logger.debug(f"Block range: {blocks_df['block_number'].min()} to {blocks_df['block_number'].max()}")

            # Prepare events data
            events_dict = {}
            if data.events:
                for event_name, event_df in data.events.items():
                    if event_df.height > 0:
                        logger.debug(f"Processing {event_df.height} events from {event_name}")
                        # Apply schema validation and casting
                        event_df = event_df.cast(self.events_schema)
                        events_dict[event_name] = event_df.sort("block_number")
                        logger.debug(f"Event block range: {event_df['block_number'].min()} to {event_df['block_number'].max()}")

            return blocks_df, events_dict

        except Exception as e:
            logger.error(f"Error preparing data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    @abstractmethod
    async def write(self, data: Data) -> None:
        """Write data to target"""
        pass 