import logging
from pathlib import Path
from datetime import datetime
import polars as pl
from src.writers.base import DataWriter
from src.types.data import Data
from src.schemas.blockchain_schemas import EVENTS, BLOCKS
from src.utils.schema_converter import SchemaConverter

logger = logging.getLogger(__name__)

class ParquetWriter(DataWriter):
    """Writer for writing data to local parquet files"""
    def __init__(self, output_dir: str):
        super().__init__()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize schemas
        self.events_schema = SchemaConverter.to_polars(EVENTS)
        self.blocks_schema = SchemaConverter.to_polars(BLOCKS)
        
        logger.info(f"Initialized ParquetWriter with output directory {output_dir}")

    async def write(self, data: Data) -> None:
        """Write data to parquet files"""
        try:
            if data.events:
                for event_name, event_df in data.events.items():
                    # Create events directory structure
                    event_dir = self.output_dir / "events" / event_name.lower()
                    event_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Generate filename with timestamp and this event's block range
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    min_block = event_df['block_number'].min()
                    max_block = event_df['block_number'].max()
                    filename = f"{timestamp}_{min_block}_{max_block}.parquet"
                    
                    # Write events
                    output_path = event_dir / filename
                    event_df.write_parquet(output_path)
                    logger.info(f"Wrote {event_df.height} {event_name} events to {output_path}")

            if data.blocks:
                for event_name, blocks_df in data.blocks.items():
                    # Create blocks directory structure
                    blocks_dir = self.output_dir / "blocks" / event_name.lower()
                    blocks_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Generate filename with timestamp and this event's block range
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    min_block = blocks_df['block_number'].min()
                    max_block = blocks_df['block_number'].max()
                    filename = f"{timestamp}_{min_block}_{max_block}.parquet"
                    
                    # Write blocks
                    output_path = blocks_dir / filename
                    blocks_df.write_parquet(output_path)
                    logger.info(f"Wrote {blocks_df.height} {event_name} blocks to {output_path}")

        except Exception as e:
            logger.error(f"Error writing parquet files: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 