import logging
import os
from typing import List, Optional
import polars as pl
from hypersync import (
    HypersyncClient, ClientConfig, StreamConfig, HexOutput, Query,
    LogSelection, FieldSelection, LogField, BlockField, ColumnMapping,
    signature_to_topic0, DataType
)
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config
from src.types.hypersync import StreamParams
from src.processors.hypersync import EventData
import os
import logging

logger = logging.getLogger(__name__)

class HypersyncIngester(DataIngester):
    """Ingests data from Hypersync"""
    def __init__(self, config: Config):
        self.config = config
        self.hypersyncapi_token = os.getenv("HYPERSYNC_API_TOKEN")
        self.client = HypersyncClient(ClientConfig(
            url=self.config.data_source[0].url,
            bearer_token=self.hypersyncapi_token,
        ))

    async def get_contract_addresses(self) -> Optional[List[pl.Series]]:
        """Fetch unique contract addresses based on identifier signatures"""
        if not self.config.contract_identifier_signatures:
            return None

        addr_series_list: List[pl.Series] = []
        for sig in self.config.contract_identifier_signatures:
            query = Query(
                from_block=self.config.from_block,
                logs=[LogSelection(topics=[[signature_to_topic0(sig)]])],
                field_selection=FieldSelection(log=[LogField.ADDRESS])
            )

            receiver = await self.client.stream_arrow(query, StreamConfig(hex_output=HexOutput.PREFIXED))
            addresses = set()

            for _ in range(5):  # Limit iterations
                res = await receiver.recv()
                if res is None:
                    break
                
                if res.data.logs is not None:
                    logs_df = pl.from_arrow(res.data.logs)
                    addresses.update(logs_df['address'].unique().to_list())

            if addresses:
                addr_series_list.append(pl.Series('address', list(addresses)))
                logger.info(f"Collected {len(addresses)} unique addresses for signature {sig}")

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
                )

                query = Query(
                    from_block=params.from_block,
                    to_block=None if params.to_block is None else params.to_block + 1,
                    logs=[LogSelection(topics=[[signature_to_topic0(event.signature)]])],
                    field_selection=FieldSelection(
                        log=[e.value for e in LogField],
                        block=[BlockField.NUMBER, BlockField.TIMESTAMP]
                    )
                )

                stream_conf = StreamConfig(
                    hex_output=HexOutput.PREFIXED,
                    event_signature=event.signature,
                    column_mapping=ColumnMapping(
                        decoded_log=event.column_mapping,
                        block={BlockField.TIMESTAMP: DataType.INT64}
                    )
                )

                receiver = await self.client.stream_arrow(query, stream_conf)
                event_data = EventData(params)
                event_dfs = []
                block_dfs = []

                for _ in range(5):
                    res = await receiver.recv()
                    if res is None:
                        break
                    
                    event_df, block_df = event_data.append_data(res)
                    if event_df is not None:
                        event_dfs.append(event_df)
                    if block_df is not None:
                        block_dfs.append(block_df)

                # Store processed data
                if event_dfs:
                    events_data[event.name] = pl.concat(event_dfs)
                if block_dfs:
                    blocks_data[event.name] = pl.concat(block_dfs)

            return Data(
                blocks=blocks_data if blocks_data else None,
                transactions=None,
                events=events_data
            )

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise