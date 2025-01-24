import logging, os
from typing import List, Optional, Dict, Tuple, AsyncGenerator
import polars as pl
from hypersync import HypersyncClient, ClientConfig
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config, Event
from src.processors.hypersync import EventData
from src.types.hypersync import StreamParams
from src.utils.generate_hypersync_query import generate_contract_query, generate_event_query
from src.loaders.base import DataLoader
from hypersync import Query, StreamConfig
import asyncio

logger = logging.getLogger(__name__)

class AsyncEventProcessor:
    """Helper class to handle async iteration of event data"""
    def __init__(self, event_processor):
        self.event_processor = event_processor
        self.events_data = {}
        self.blocks_data = {}
        logger.info(f"Initialized AsyncEventProcessor for {self.event_processor.event_name}")

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            res = await self.event_processor.receiver.recv()
            if res is None:
                # Handle any remaining data before stopping
                if self.event_processor.total_events > 0:
                    combined_events, combined_blocks = self.event_processor.get_combined_data()
                    if (combined_events is not None and not combined_events.is_empty()) or \
                       (combined_blocks is not None and not combined_blocks.is_empty()):
                        data = Data(
                            events={self.event_processor.event_name: combined_events} if combined_events is not None else None,
                            blocks={self.event_processor.event_name: combined_blocks} if combined_blocks is not None else None,
                            transactions=None
                        )
                        logger.info(f"Final batch for {self.event_processor.event_name}: "
                                  f"{combined_events.height if combined_events is not None else 0} events, "
                                  f"{combined_blocks.height if combined_blocks is not None else 0} blocks")
                        return data
                raise StopAsyncIteration

            event_df, block_df, should_write = self.event_processor.append_data(res)
            
            if should_write:
                combined_events, combined_blocks = self.event_processor.get_combined_data()
                if (combined_events is not None and not combined_events.is_empty()) or \
                   (combined_blocks is not None and not combined_blocks.is_empty()):
                    data = Data(
                        events={self.event_processor.event_name: combined_events} if combined_events is not None else None,
                        blocks={self.event_processor.event_name: combined_blocks} if combined_blocks is not None else None,
                        transactions=None
                    )
                    # Update current block to next block from response
                    self.event_processor.current_block = res.next_block
                    return data
            
            return None

        except Exception as e:
            logger.error(f"Error processing event data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

class HypersyncIngester(DataIngester):
    """Ingests data from Hypersync"""
    def __init__(self, config: Config):
        self.config = config
        self.hypersyncapi_token = os.getenv("HYPERSYNC_API_TOKEN")
        self.client = HypersyncClient(ClientConfig(
            url=self.config.data_source[0].url,
            bearer_token=self.hypersyncapi_token,
        ))
        self._current_block = config.blocks.range.from_block
        self._contract_addr_list = None
        self._event_queries = {}
        self._event_processors = {}  # Cache for event processors
        self._loaders = None
        logger.info("Initialized HypersyncIngester")

    @property
    def current_block(self) -> int:
        """Get current block number"""
        return self._current_block

    @current_block.setter
    def current_block(self, value: int):
        """Set current block number"""
        self._current_block = value

    async def get_contract_addr_list(self) -> Optional[List[pl.Series]]:
        """Get or return cached contract addresses"""
        if self._contract_addr_list is not None:
            return self._contract_addr_list

        if not self.config.contracts.identifier_signatures:
            return None

        logger.info("Getting contract address lists (one-time operation)")
        addr_series_list: List[pl.Series] = []

        for contract in self.config.contracts.identifier_signatures:
            query, stream_config = generate_contract_query(
                contract.signature, 
                self.config.blocks.contract_discovery.from_block
            )
            receiver = await self.client.stream_arrow(query, stream_config)
            addresses = set()

            try:
                while True:
                    res = await receiver.recv()
                    if res is None: break
                    if res.data.logs is not None:
                        logs_df = pl.from_arrow(res.data.logs)
                        addresses.update(logs_df['address'].unique().to_list())

                if addresses:
                    addr_series_list.append(pl.Series('address', list(addresses)))
                    logger.info(f"Collected {len(addresses)} unique addresses for signature {contract.signature}")
            except Exception as e:
                logger.error(f"Error collecting addresses for signature {contract.signature}: {e}")
                raise

        if addr_series_list:
            self._contract_addr_list = addr_series_list
            total_addresses = sum(len(series) for series in addr_series_list)
            logger.info(f"Generated contract address list with {total_addresses} total unique addresses")
            return addr_series_list
        
        logger.info("No contract addresses found")
        return None

    async def initialize_loaders(self, loaders: Dict[str, DataLoader]):
        """Initialize data loaders"""
        self._loaders = loaders
        logger.info(f"Initialized {len(loaders)} data loaders: {', '.join(loaders.keys())}")

    async def _write_to_targets(self, data: Data) -> None:
        """Write data to configured targets in parallel"""
        if not self._loaders:
            logger.error("No loaders initialized")
            return

        if not data.events or not any(df.height > 0 for df in data.events.values()):
            logger.info("No data to write")
            return

        try:
            # Create separate copies for each loader
            loader_data = {}
            for loader_type in self._loaders.keys():
                loader_data[loader_type] = Data(
                    events={name: df.clone() for name, df in data.events.items()} if data.events else None,
                    blocks={name: df.clone() for name, df in data.blocks.items()} if data.blocks else None,
                    transactions=data.transactions
                )

            # Create tasks for all loaders to write in parallel
            write_tasks = {
                loader_type: asyncio.create_task(
                    loader.write_data(loader_data[loader_type]),
                    name=f"write_{loader_type}"
                )
                for loader_type, loader in self._loaders.items()
            }
            
            if write_tasks:
                logger.info(f"Writing in parallel to {len(write_tasks)} targets ({', '.join(self._loaders.keys())})")
                
                # Wait for all writes to complete concurrently
                results = await asyncio.gather(
                    *write_tasks.values(), 
                    return_exceptions=True
                )
                
                # Check for any errors
                for loader_type, result in zip(write_tasks.keys(), results):
                    if isinstance(result, Exception):
                        logger.error(f"Error in {loader_type} writer: {result}")
                        raise result

        except Exception as e:
            logger.error(f"Error during parallel write: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    async def _get_event_query(self, event: Event) -> Tuple[Query, StreamConfig, StreamParams]:
        """Get cached query or create new one"""
        if event.name in self._event_queries:
            query, stream_config, stream_params = self._event_queries[event.name]
            # Ensure we don't modify the cached query's from_block
            query = Query(**{**query.__dict__, 'from_block': self.current_block})
            stream_params = StreamParams(**{**stream_params.__dict__, 'from_block': self.current_block})
            return query, stream_config, stream_params

        logger.info(f"Generating query for event {event.name} (one-time operation)")
        contract_addr_list = await self.get_contract_addr_list()
        query, stream_config, stream_params = generate_event_query(
            self.config, event, self.client, contract_addr_list, self.current_block,
            self.config.processing.items_per_batch
        )
        self._event_queries[event.name] = (query, stream_config, stream_params)
        logger.info(f"Cached query configuration for event {event.name}")
        return query, stream_config, stream_params

    async def get_data(self, from_block: int) -> AsyncGenerator[Data, None]:
        """Process blockchain data and yield Data objects when batch is ready"""
        try:
            await self.get_contract_addr_list()

            for event in self.config.events:
                logger.info(f"Processing event: {event.name} from block {from_block}")
                
                # Get or create event processor
                if event.name not in self._event_processors:
                    query, stream_config, stream_params = await self._get_event_query(event)
                    stream_params.from_block = from_block
                    
                    receiver = await self.client.stream_arrow(query, stream_config)
                    event_data = EventData(stream_params)
                    event_data.receiver = receiver
                    event_data.from_block = from_block
                    logger.info(f"Initialized data processor for {event.name}")

                    self._event_processors[event.name] = AsyncEventProcessor(event_data)
                    logger.info(f"Initialized AsyncEventProcessor for {event.name}")
                
                # Use existing processor
                async_processor = self._event_processors[event.name]
                async for data in async_processor:
                    if data is not None:
                        self._current_block = async_processor.event_processor.current_block
                        logger.debug(f"Updated current block to {self._current_block} for {event.name}")
                        yield data

        except Exception as e:
            logger.error(f"Error processing data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 
