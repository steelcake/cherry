from dataclasses import dataclass
import logging
import sys
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
    signature_to_topic0
)
import polars as pl
from typing import Dict, List, cast, Optional
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config
from src.schemas.blockchain_schemas import EVENTS
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

@dataclass
class GetParquetParams:
    client: HypersyncClient
    column_mapping: Dict[str, hypersync.DataType]
    event_name: str
    signature: str
    contract_addr_list: Optional[List[pl.Series]]
    from_block: int
    to_block: Optional[int]
    items_per_section: int

class HypersyncIngester(DataIngester):
    def __init__(self, config: Config):
        self.config = config   
        logger.info("API Token retrieved successfully" 
                     if (hypersyncapi_token := os.getenv('HYPERSYNC_API_TOKEN')) 
                     else "API Token not found")
        logger.info(f"Initializing HypersyncIngester with URL: {config.data_source[0].url}")
        self.client = HypersyncClient(ClientConfig(
            url=config.data_source[0].url,
            bearer_token=hypersyncapi_token,
        ))
        self.events = {event.name: event for event in config.events}

    async def get_contract_addr_list(self) -> Optional[List[pl.Series]]:
        try:
            if not self.config.contract_identifier_signatures:
                return None

            addr_series_list: List[pl.Series] = []
            
            for sig in self.config.contract_identifier_signatures:
                topic0 = signature_to_topic0(sig)
                query = Query(
                    from_block=0,
                    logs=[LogSelection(
                        topics=[[topic0]]
                    )],
                    field_selection=FieldSelection(
                        log=[LogField.ADDRESS]
                    ),
                )
                
                logger.info(f"Processing signature {sig}")
                
                stream_conf = StreamConfig(
                    hex_output=HexOutput.PREFIXED,
                )
                logger.info("Stream conf created")

                receiver = await self.client.stream_arrow(query, stream_conf)
                addresses = set()
                
                for i in range(10): # TODO: while True
                    res = await receiver.recv()
                    if res is None:
                        break
                        
                    if res.data.logs is not None:
                        logs_df = pl.from_arrow(res.data.logs)
                        addresses.update(logs_df['address'].unique().to_list())
                
                if addresses:
                    addr_series_list.append(pl.Series('address', list(addresses)))
                
            logger.info(f"addr_series_list: {addr_series_list}")
            return addr_series_list if addr_series_list else None
            
        except Exception as e:
            logger.error(f"Error getting contract addresses: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            return None

    async def get_data(self, from_block: int, to_block: int) -> Data:
        try:
            events_data: Dict[str, pl.DataFrame] = {}
            contract_addr_list = await self.get_contract_addr_list()
            logger.info(f"contract_addr_list: {contract_addr_list}")

            for event in self.config.events:
                try:
                    logger.info(f"Processing {event.name}")
                    logger.info(f"signature is {event.signature}")
                    
                    topic0 = signature_to_topic0(event.signature)
                    logger.info(f"topic0 is {topic0}")
                    
                    query = Query(
                        from_block=from_block,
                        to_block=None if self.config.to_block is None else self.config.to_block + 1,
                        logs=[LogSelection(
                            topics=[[
                                topic0
                            ]]
                        )],
                        field_selection=FieldSelection(
                            log=[e.value for e in LogField],
                            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
                        ),
                    )
                    logger.info(f"HypersyncQuery: {query}")
                    logger.info(f"Event signature: {event.signature}")

                    stream_conf = StreamConfig(
                        hex_output=HexOutput.PREFIXED,
                        event_signature=event.signature,
                        column_mapping=ColumnMapping(
                            decoded_log=event.column_mapping,
                            block={
                                BlockField.TIMESTAMP: hypersync.DataType.INT64,
                            }
                        ),
                    )

                    receiver = await self.client.stream_arrow(query, stream_conf)
                    event_logs = []

                    for i in range(10): # TODO: while True
                        res = await receiver.recv()
                        logger.info(res)
                        if res is None:
                            break
                            
                        logger.info(f"res.data.logs: {res.data.logs}")
                        if res.data.logs is not None:
                            logs_df = pl.from_arrow(res.data.logs)
                            decoded_logs_df = pl.from_arrow(res.data.decoded_logs)
                            blocks_df = pl.from_arrow(res.data.blocks)
                            logger.info(f"logs_df: {logs_df}")
                            logger.info(f"decoded_logs_df: {decoded_logs_df}")
                            logger.info(f"blocks_df: {blocks_df}")


                            decoded_logs_df = decoded_logs_df.rename(lambda n: f"decoded_{n}")
                            blocks_df = blocks_df.rename(lambda n: f"block_{n}")

                            combined_df = logs_df.hstack(decoded_logs_df).join(blocks_df, on="block_number")
                            
                            if combined_df.height > 0:
                                event_logs.append(combined_df)
                                logger.info(f"processed up to block {res.next_block}")
                    
                    logger.info(f"event_logs: {event_logs}")
                    if event_logs:
                        events_data[event.name] = pl.concat(event_logs)
                    else:
                        events_data[event.name] = pl.DataFrame()

                except Exception as e:
                    logger.error(f"Error processing event {event.name}: {e}")
                    logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
                    events_data[event.name] = pl.DataFrame()

            logger.info(f"events_data: {events_data[event.name][:2]}")
            logger.info(f"events_data keys: {events_data.keys()}")
            logger.info(f"events_data dtypes: {events_data[event.name].dtypes}")
            logger.info(f"events_data columns: {events_data[event.name].columns}")
            logger.info(f"Polars schema: {EVENTS.to_polars()}")
            # Create empty blocks DataFrame if no events were processed
            logger.info("Creating empty blocks DataFrame")
            blocks_df = pl.DataFrame(schema={"block_number": pl.Int64, "block_timestamp": pl.Int64})
            events_df = pl.DataFrame(events_data[event.name], schema=EVENTS.to_polars())

            blocks_df.write_parquet(f"data/blocks_{from_block}_{to_block}.parquet")
            events_df.write_parquet(f"data/events_{from_block}_{to_block}.parquet")

            return Data(
                blocks=blocks_df,
                transactions=None,
                events=events_df
            )

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise