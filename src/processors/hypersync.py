import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import polars as pl
from hypersync import ArrowResponse, DataType
from src.types.hypersync import StreamParams
from src.schemas.blockchain_schemas import BLOCKS, EVENTS, EVENT_SCHEMAS
from src.schemas.base import SchemaConverter
import time

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
        self.events_schema = SchemaConverter.to_polars(EVENT_SCHEMAS[self.event_name])
        self.blocks_schema = SchemaConverter.to_polars(BLOCKS)
        self.total_events = 0
        self.items_per_section = params.items_per_section
        self.column_mapping = params.column_mapping
        self.last_process_time = time.time()
        self.last_event_count = 0
        logger.info(f"Initialized EventData processor for {self.event_name} from block {self.from_block} to {self.to_block or 'latest'}")

    def _convert_hypersync_type(self, dtype: str) -> pl.DataType:
        """Convert Hypersync DataType string to Polars DataType"""
        type_mapping = {
            "float64": pl.Float64,
            "int64": pl.Int64,
            "string": pl.Utf8,
            "bool": pl.Boolean,
            "bytes": pl.Binary
        }
        return type_mapping.get(str(dtype).lower(), pl.Utf8)

    def append_data(self, res: ArrowResponse) -> bool:
        """Process and append new data"""
        try:
            if res is None:
                return False

            # Update current block from response
            self.current_block = res.next_block
            
            # Convert Arrow data to Polars with column mapping
            logs_df = pl.from_arrow(res.data.logs)
            if logs_df.height == 0:
                return False

            decoded_logs_df = pl.from_arrow(res.data.decoded_logs).rename(lambda n: f"decoded_{n}")
            blocks_df = pl.from_arrow(res.data.blocks).rename(lambda n: f"block_{n}")

            # Apply column mappings from hypersync config
            if self.column_mapping:
                for col, dtype in self.column_mapping.items():
                    decoded_col = f"decoded_{col}"
                    if decoded_col in decoded_logs_df.columns:
                        polars_type = self._convert_hypersync_type(dtype)
                        logger.debug(f"Converting column {decoded_col} from {dtype} to {polars_type}")
                        decoded_logs_df = decoded_logs_df.with_columns(pl.col(decoded_col).cast(polars_type))

            # Join and validate schemas
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
                
                # Calculate and log processing speed
                current_time = time.time()
                time_diff = current_time - self.last_process_time
                if time_diff >= 5:  # Log every 5 seconds
                    events_diff = self.total_events - self.last_event_count
                    speed = events_diff / time_diff
                    blocks_per_second = (self.current_block - self.from_block) / time_diff
                    logger.info(f"Event: {self.event_name} - Processed {combined_df.height} events "
                              f"(Total: {self.total_events}/{self.items_per_section}), "
                              f"Speed: {speed:.0f} events/s, {blocks_per_second:.0f} blocks/s")
                    self.last_process_time = current_time
                    self.last_event_count = self.total_events

            if blocks_df.height > 0:
                self.blocks_df_list.append(blocks_df)

            # Check if we should write based on block progress
            should_write = (
                self.total_events >= self.items_per_section or 
                (self.to_block and self.current_block >= self.to_block)
            )

            return should_write

        except Exception as e:
            logger.error(f"Error processing data chunk: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    def get_combined_data(self) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
        """Combine and return all collected data"""
        events_df = pl.concat(self.logs_df_list) if self.logs_df_list else None
        blocks_df = pl.concat(self.blocks_df_list) if self.blocks_df_list else None
        
        if events_df is not None:
            logger.info(f"Combined {len(self.logs_df_list)} event dataframes, total rows: {events_df.height}")
        if blocks_df is not None:
            logger.info(f"Combined {len(self.blocks_df_list)} block dataframes, total rows: {blocks_df.height}")
            
        self.logs_df_list = []
        self.blocks_df_list = []
        self.total_events = 0
        return events_df, blocks_df