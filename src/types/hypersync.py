from dataclasses import dataclass
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import polars as pl
from hypersync import HypersyncClient, ArrowResponse, DataType

logger = logging.getLogger(__name__)

@dataclass
class StreamParams:
    """Parameters for streaming data from Hypersync"""
    client: HypersyncClient
    column_mapping: Dict[str, DataType]
    event_name: str
    signature: str
    contract_addr_list: Optional[List[pl.Series]]
    from_block: int
    to_block: Optional[int]
    output_dir: Optional[str] = None

class EventData:
    """Handles event data processing and storage"""
    def __init__(self, params: StreamParams):
        self.event_name = params.event_name
        self.from_block = params.from_block
        self.to_block = params.from_block
        self.logs_df_list = []
        self.blocks_df_list = []
        self.contract_addr_list = params.contract_addr_list
        self.output_dir = params.output_dir

    def append_data(self, res: ArrowResponse) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
        """Process and append new data, return the processed DataFrames"""
        self.to_block = res.next_block
        logs_df = pl.from_arrow(res.data.logs)
        decoded_logs_df = pl.from_arrow(res.data.decoded_logs).rename(lambda n: f"decoded_{n}")
        blocks_df = pl.from_arrow(res.data.blocks).rename(lambda n: f"block_{n}")

        logger.debug(f"Raw data - Logs: {logs_df.height}, Decoded Logs: {decoded_logs_df.height}, Blocks: {blocks_df.height}")
        
        # Join the dataframes
        combined_df = logs_df.hstack(decoded_logs_df).join(blocks_df, on="block_number")

        if self.contract_addr_list:
            for addr_filter in self.contract_addr_list:
                combined_df = combined_df.filter(pl.col("address").is_in(addr_filter))

        # Store processed data
        if combined_df.height > 0:
            self.logs_df_list.append(combined_df)
            logger.info(f"Processed {combined_df.height} events for {self.event_name}")

        if blocks_df.height > 0:
            self.blocks_df_list.append(blocks_df)
            logger.info(f"Processed {blocks_df.height} blocks for {self.event_name}")

        return combined_df if combined_df.height > 0 else None, blocks_df if blocks_df.height > 0 else None

    def write_parquet(self) -> None:
        """Write accumulated data to parquet files"""
        if not self.output_dir:
            logger.warning("No output directory specified, skipping parquet write")
            return

        try:
            output_path = Path(self.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Write events data
            if self.logs_df_list:
                events_df = pl.concat(self.logs_df_list)
                events_file = output_path / f"{self.event_name}_{timestamp}_{self.from_block}_{self.to_block}_events.parquet"
                events_df.write_parquet(str(events_file))
                logger.info(f"Wrote {events_df.height} events to {events_file}")

            # Write blocks data
            if self.blocks_df_list:
                blocks_df = pl.concat(self.blocks_df_list)
                blocks_file = output_path / f"{self.event_name}_{timestamp}_{self.from_block}_{self.to_block}_blocks.parquet"
                blocks_df.write_parquet(str(blocks_file))
                logger.info(f"Wrote {blocks_df.height} blocks to {blocks_file}")

        except Exception as e:
            logger.error(f"Error writing parquet files: {e}")            
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            
