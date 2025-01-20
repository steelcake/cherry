import logging, os
from typing import List, Optional
import polars as pl
from hypersync import HypersyncClient, ClientConfig
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config
from src.processors.hypersync import EventData
from src.utils.generate_hypersync_query import generate_contract_query, generate_event_query

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
        logger.info("Initialized HypersyncIngester")

    async def get_contract_addresses(self) -> Optional[List[pl.Series]]:
        """Fetch unique contract addresses based on identifier signatures"""
        if not self.config.contract_identifier_signatures:
            return None

        addr_series_list: List[pl.Series] = []
        for sig in self.config.contract_identifier_signatures:
            query, stream_config = generate_contract_query(sig, self.config.from_block)
            receiver = await self.client.stream_arrow(query, stream_config)
            addresses = set()

            for _ in range(5):  # Limit iterations
                res = await receiver.recv()
                if res is None: break
                
                if res.data.logs is not None:
                    logs_df = pl.from_arrow(res.data.logs)
                    addresses.update(logs_df['address'].unique().to_list())

            if addresses:
                addr_series_list.append(pl.Series('address', list(addresses)))
                logger.info(f"Collected {len(addresses)} unique addresses for signature {sig}")

        return addr_series_list if addr_series_list else None

    async def get_data(self, from_block: int) -> Data:
        """Fetch and process blockchain data for the specified block range"""
        try:
            events_data, blocks_data = {}, {}
            contract_addr_list = await self.get_contract_addresses()
            logger.debug(f"Contract address list: {contract_addr_list}")
            logger.info(f"Processing blocks {from_block} to ?")
            
            for event in self.config.events:
                logger.info(f"Processing event: {event.name}")
                query, stream_config, stream_params = generate_event_query(
                    self.config, event, self.client, contract_addr_list, from_block
                )

                receiver = await self.client.stream_arrow(query, stream_config)
                event_data = EventData(stream_params)
                event_dfs, block_dfs = [], []

                for _ in range(5):
                    res = await receiver.recv()
                    if res is None: break
                    
                    event_df, block_df = event_data.append_data(res)
                    if event_df is not None: event_dfs.append(event_df)
                    if block_df is not None: block_dfs.append(block_df)

                if event_dfs: events_data[event.name] = pl.concat(event_dfs)
                if block_dfs: blocks_data[event.name] = pl.concat(block_dfs)

            return Data(
                blocks=blocks_data if blocks_data else None,
                transactions=None,
                events=events_data
            )

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise