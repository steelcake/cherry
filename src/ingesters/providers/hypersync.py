from dataclasses import dataclass
import logging
from pathlib import Path
import hypersync
from hypersync import (
    HypersyncClient, 
    ClientConfig,
    StreamConfig,
    HexOutput,
    Query,
    LogSelection,
    FieldSelection,
    LogField,
    BlockField,
    ColumnMapping,
    signature_to_topic0,
    ArrowResponse
)
import polars as pl
from typing import Dict, List, Optional, Tuple
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

@dataclass
class StreamParams:
    client: HypersyncClient
    column_mapping: Dict[str, hypersync.DataType]
    event_name: str
    signature: str
    contract_addr_list: Optional[List[pl.Series]]
    from_block: int
    to_block: Optional[int]
    output_dir: Optional[str] = None

class EventData:
    def __init__(self, params: StreamParams):
        self.event_name = params.event_name
        self.from_block = params.from_block
        self.to_block = params.from_block
        self.logs_df_list = []
        self.blocks_df_list = []
        self.contract_addr_list = params.contract_addr_list
        self.output_dir = params.output_dir
        logger.info(f"Initialized EventData for {self.event_name}")

    def append_data(self, res: ArrowResponse) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
        """Process and append new data, return the processed DataFrames"""
        self.to_block = res.next_block
        logs_df = pl.from_arrow(res.data.logs)
        decoded_logs_df = pl.from_arrow(res.data.decoded_logs)
        blocks_df = pl.from_arrow(res.data.blocks)

        logger.info(f"Logs: {logs_df}")
        logger.info(f"Logs columns: {logs_df.columns}")

        logger.debug(f"Raw data - Logs: {logs_df.height}, Decoded: {decoded_logs_df.height}, Blocks: {blocks_df.height}")

        # Process events data
        decoded_logs_df = decoded_logs_df.rename(lambda n: f"decoded_{n}")
        blocks_df = blocks_df.rename(lambda n: f"block_{n}")

        logger.info(f"Decoded logs: {decoded_logs_df}")
        logger.info(f"Decoded logs columns: {decoded_logs_df.columns}")
        logger.info(f"Blocks: {blocks_df}")
        
        # Join the dataframes
        combined_df = logs_df.hstack(decoded_logs_df).join(blocks_df, on="block_number")

        logger.info(f"Combined df: {combined_df}")
        logger.info(f"Combined df columns: {combined_df.columns}")

        logger.info(f"Contract addr list: {self.contract_addr_list}")


        # Apply contract address filtering if needed
        if self.contract_addr_list is not None:
            for addr_filter in self.contract_addr_list:
                combined_df = combined_df.filter(pl.col("address").is_in(addr_filter))
        
        logger.info(f"Combined df after filtering: {combined_df}")

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
        try:
            if not self.output_dir:
                logger.warning("No output directory specified, skipping parquet write")
                return

            output_path = Path(self.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            # Write events data
            if self.logs_df_list:
                events_df = pl.concat(self.logs_df_list)
                events_file = output_path / f"{self.event_name}_{self.from_block}_{self.to_block}_events.parquet"
                events_df.write_parquet(str(events_file))  # Convert Path to string
                logger.info(f"Wrote {events_df.height} events to {events_file}")

            # Write blocks data
            if self.blocks_df_list:
                blocks_df = pl.concat(self.blocks_df_list)
                blocks_file = output_path / f"{self.event_name}_{self.from_block}_{self.to_block}_blocks.parquet"
                blocks_df.write_parquet(str(blocks_file))  # Convert Path to string
                logger.info(f"Wrote {blocks_df.height} blocks to {blocks_file}")

        except Exception as e:
            logger.error(f"Error writing parquet files: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")

class HypersyncIngester(DataIngester):
    def __init__(self, config: Config):
        self.config = config
        self.output_dir = self.config.output[1].output_dir
        self.hypersyncapi_token = os.getenv("HYPERSYNC_API_TOKEN")
        self.client = HypersyncClient(ClientConfig(
            url=self.config.data_source[0].url,
            bearer_token=self.hypersyncapi_token,
        ))
        self.events = {event.name: event for event in config.events}

        if self.output_dir:
            logger.info(f"Output directory set to: {self.output_dir}")
        else:
            logger.warning("No output directory specified in config")

    async def get_contract_addresses(self) -> Optional[List[pl.Series]]:
        """Fetch unique contract addresses based on identifier signatures"""
        if not self.config.contract_identifier_signatures:
            return None

        addr_series_list: List[pl.Series] = []
        for sig in self.config.contract_identifier_signatures:
            topic0 = signature_to_topic0(sig)
            query = hypersync.Query(
                from_block=self.config.from_block,
                logs=[LogSelection(
                    topics=[[topic0]]
                )],
                field_selection=FieldSelection(
                    log=[LogField.ADDRESS]
                ),
            )

            stream_conf = StreamConfig(hex_output=HexOutput.PREFIXED)
            receiver = await self.client.stream_arrow(query, stream_conf)
            addresses = set()

            while True:  # Limit iterations
                res = await receiver.recv()
                if res is None:
                    break
                
                if res.data.logs is not None:
                    logs_df = pl.from_arrow(res.data.logs)
                    new_addresses = logs_df['address'].unique().to_list()
                    addresses.update(new_addresses)
                    logger.debug(f"Found {len(new_addresses)} new addresses, total: {len(addresses)}")

            if addresses:
                addr_series_list.append(pl.Series('address', list(addresses)))
                logger.info(f"Collected {len(addresses)} unique addresses for signature {sig}")

        logger.info(f"Contract addresses obtained: {addr_series_list}")
        return addr_series_list if addr_series_list else None

    async def get_data(self, from_block: int, to_block: int) -> Data:
        """Fetch and process blockchain data for the specified block range"""
        try:
            events_data = {}
            blocks_data = {}
            contract_addr_list = await self.get_contract_addresses()
            
            logger.info(f"Processing blocks {from_block} to {to_block}")
            
            for event in self.config.events:
                logger.info(f"Processing event: {event.name}")
                
                params = StreamParams(
                    client=self.client,
                    column_mapping=event.column_mapping,
                    event_name=event.name,
                    signature=event.signature,
                    contract_addr_list=contract_addr_list,
                    from_block=self.config.from_block,
                    to_block=None if self.config.to_block is None else self.config.to_block + 1,
                    output_dir=self.output_dir  # Pass output_dir from config
                )

                topic0 = signature_to_topic0(event.signature)
                query = Query(
                    from_block=params.from_block,
                    to_block=None if params.to_block is None else params.to_block + 1,
                    logs=[LogSelection(
                        topics = [[
                            topic0
                        ]]
                    )],
                    field_selection=FieldSelection(
                        log=[e.value for e in LogField],
                        block=[BlockField.NUMBER, BlockField.TIMESTAMP],
                        # block=[e.value for e in BlockField],
                        # transaction=[e.value for e in TransactionField],
                    ),
                )

                stream_conf = StreamConfig(
                    hex_output=HexOutput.PREFIXED,
                    event_signature=event.signature,
                    column_mapping=ColumnMapping(
                        decoded_log=event.column_mapping,
                        block={BlockField.TIMESTAMP: hypersync.DataType.INT64}
                    )
                )

                receiver = await self.client.stream_arrow(query, stream_conf)
                event_data = EventData(params)
                event_dfs = []
                block_dfs = []

                while True:
                    res = await receiver.recv()
                    if res is None:
                        break
                    
                    event_df, block_df = event_data.append_data(res)
                    if event_df is not None:
                        event_dfs.append(event_df)
                    if block_df is not None:
                        block_dfs.append(block_df)

                # Write to parquet after processing all data for this event
                event_data.write_parquet()

                # Store processed data
                if event_dfs:
                    events_data[event.name] = pl.concat(event_dfs)
                    logger.info(f"Total events for {event.name}: {events_data[event.name].height}")
                if block_dfs:
                    blocks_data[event.name] = pl.concat(block_dfs)
                    logger.info(f"Total blocks for {event.name}: {blocks_data[event.name].height}")

            return Data(
                blocks=blocks_data if blocks_data else None,
                transactions=None,
                events=events_data
            )

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise