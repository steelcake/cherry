import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import polars as pl
from hypersync import ArrowResponse
from src.types.hypersync import StreamParams
from src.schemas.blockchain_schemas import BLOCKS, EVENTS
from src.schemas.base import SchemaConverter

logger = logging.getLogger(__name__)

class EventData:
    """Handles event data processing and storage"""
    def __init__(self, params: StreamParams):
        self.event_name = params.event_name
        self.from_block = params.from_block
        self.to_block = params.to_block
        self.current_block = params.from_block
        self.logs_df_list = []
        self.blocks_df_list = []
        self.contract_addr_list = params.contract_addr_list
        self.events_schema = SchemaConverter.to_polars(EVENTS)
        self.blocks_schema = SchemaConverter.to_polars(BLOCKS)
        self.total_events = 0
        self.items_per_section = params.items_per_section
        self.column_mapping = params.column_mapping
        self.receiver = None
        logger.info(f"Initialized EventData processor for {self.event_name} from block {self.from_block} to {self.to_block or 'latest'}")

    def append_data(self, res: ArrowResponse) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame], bool]:
        """Process and append new data"""
        try:
            if res is None:
                return None, None, True

            self.current_block = res.next_block
            logger.debug(f"Processing data chunk from block {self.from_block} to {self.current_block}")
            
            # Convert Arrow data to Polars with column mapping
            logs_df = pl.from_arrow(res.data.logs)
            decoded_logs_df = pl.from_arrow(res.data.decoded_logs).rename(lambda n: f"decoded_{n}")
            blocks_df = pl.from_arrow(res.data.blocks).rename(lambda n: f"block_{n}")

            if logs_df.height == 0:
                return None, None, False

            # Apply column mappings from hypersync config
            if self.column_mapping:
                for col, dtype in self.column_mapping.items():
                    if col in decoded_logs_df.columns:
                        decoded_logs_df = decoded_logs_df.with_columns(pl.col(col).cast(dtype))

            # Join and validate schemas
            logger.debug("Joining and validating data schemas")
            combined_df = logs_df.hstack(decoded_logs_df).join(blocks_df, on="block_number")
            combined_df = combined_df.cast(self.events_schema)
            blocks_df = blocks_df.cast(self.blocks_schema)

            if self.contract_addr_list:
                for addr_filter in self.contract_addr_list:
                    combined_df = combined_df.filter(pl.col("address").is_in(addr_filter))

            # Update total events count
            if combined_df.height > 0:
                self.total_events += combined_df.height
                self.logs_df_list.append(combined_df)
                logger.info(f"Event: {self.event_name} - Processed {combined_df.height} events (Total in batch: {self.total_events}/{self.items_per_section})")

            if blocks_df.height > 0:
                self.blocks_df_list.append(blocks_df)
                logger.debug(f"Added {blocks_df.height} blocks")

            # Check if we've hit the batch limit or reached target block
            should_write = (
                self.total_events >= self.items_per_section or 
                (self.to_block and self.current_block >= self.to_block)
            )
            
            if should_write:
                logger.info(f"Batch size limit reached ({self.total_events}/{self.items_per_section} events)")

            return combined_df, blocks_df, should_write

        except Exception as e:
            logger.error(f"Error processing data chunk: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    def get_combined_data(self) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
        """Combine and return all collected data"""
        events_df = None
        blocks_df = None
        
        if self.logs_df_list:
            events_df = pl.concat(self.logs_df_list)
            logger.info(f"Combined {len(self.logs_df_list)} event dataframes, total rows: {events_df.height}")
            self.logs_df_list = []
            
        if self.blocks_df_list:
            blocks_df = pl.concat(self.blocks_df_list)
            logger.info(f"Combined {len(self.blocks_df_list)} block dataframes, total rows: {blocks_df.height}")
            self.blocks_df_list = []
        
        self.total_events = 0
        return events_df, blocks_df

    def write_parquet(self) -> None:
        """Write accumulated data to parquet files"""
        try:
            output_path = Path(self.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Write events data
            if self.logs_df_list:
                events_df = pl.concat(self.logs_df_list)
                # Apply any final transformations from column mapping
                if self.column_mapping:
                    for col, dtype in self.column_mapping.items():
                        if col in events_df.columns:
                            events_df = events_df.with_columns(pl.col(col).cast(dtype))
                
                events_file = output_path / f"{self.event_name}_{timestamp}_{self.from_block}_{self.to_block}_events.parquet"
                events_df.write_parquet(str(events_file))
                logger.info(f"Wrote {events_df.height} events to {events_file}")
                logger.debug(f"Event block range: {events_df['block_number'].min()} to {events_df['block_number'].max()}")

            # Write blocks data
            if self.blocks_df_list:
                blocks_df = pl.concat(self.blocks_df_list)
                blocks_df = blocks_df.unique(subset=["block_number"]).sort("block_number")
                blocks_file = output_path / f"{self.event_name}_{timestamp}_{self.from_block}_{self.to_block}_blocks.parquet"
                blocks_df.write_parquet(str(blocks_file))
                logger.info(f"Wrote {blocks_df.height} unique blocks to {blocks_file}")
                logger.debug(f"Block range: {blocks_df['block_number'].min()} to {blocks_df['block_number'].max()}")

        except Exception as e:
            logger.error(f"Error writing parquet files: {e}")            
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise