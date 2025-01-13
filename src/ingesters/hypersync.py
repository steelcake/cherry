import json, logging, aiohttp
import polars as pl
import pyarrow as pa
import cryo
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config
from src.utils.logging_setup import setup_logging
from src.schemas.blockchain_schemas import BLOCKS, TRANSACTIONS, EVENTS

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

class HypersyncIngester(DataIngester):
    def __init__(self, config: Config):
        self.config = config
        self.url = config.data_source[0].url
        self.events = {event.name: event for event in config.events}
        
        # Store event configurations for logging
        self.event_addresses = {event.name: event.address for event in config.events}
        self.event_topics = {event.name: event.topics for event in config.events}
        logger.debug(f"Event addresses: {json.dumps(self.event_addresses, indent=2)}")
        logger.debug(f"Event topics: {json.dumps(self.event_topics, indent=2)}")
        
        logger.info(f"Initialized HypersyncIngester with URL: {self.url}")

    async def get_data(self, from_block: int, to_block: int) -> Data:
        try:
            # Fetch blocks using cryo with polars format and convert to Arrow
            blocks_range = [f"{from_block}:{to_block}"]
            blocks_df = await cryo.async_collect(
                "blocks",
                blocks=blocks_range,
                rpc=self.url,
                output_format="polars",
                hex=True
            )
            blocks_table = pa.Table.from_pandas(blocks_df.to_pandas(), schema=BLOCKS.to_arrow())
            
            # Fetch events using eth_getLogs
            events_data = {}
            logger.info(f"Starting event fetching for {len(self.events)} event types")
            logger.info(f"Event configurations: {json.dumps({name: {'address': event.address, 'topics': event.topics} for name, event in self.events.items()}, indent=2)}")
            
            async with aiohttp.ClientSession() as session:
                for event_name, event in self.events.items():
                    query = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_getLogs",
                        "params": [{
                            "fromBlock": hex(from_block),
                            "toBlock": hex(to_block),
                            "address": event.address[0] if isinstance(event.address, list) else event.address,
                            "topics": [event.topics[0][0]] if event.topics else None
                        }]
                    }
                    
                    logger.info(f"Fetching {event_name} events with query: {json.dumps(query, indent=2)}")

                    async with session.post(self.url, json=query) as response:
                        result = await response.json()
                        
                        if 'result' in result:
                            logs = result['result']
                            logger.info(f"Found {len(logs)} logs for event {event_name}")
                            if logs:
                                logger.debug(f"Sample log for {event_name}: {json.dumps(logs[0], indent=2)}")
                                
                                # Convert to Polars DataFrame first
                                events_df = pl.DataFrame({
                                    "transaction_hash": [log['transactionHash'] for log in logs],
                                    "block_number": [int(log['blockNumber'], 16) for log in logs],
                                    "from_address": [f"0x{log['topics'][1][-40:]}" if len(log['topics']) > 1 else None for log in logs],
                                    "to_address": [f"0x{log['topics'][2][-40:]}" if len(log['topics']) > 2 else None for log in logs],
                                    "value": [int(log['data'], 16) if log['data'] != '0x' else 0 for log in logs],
                                    "event_name": [event_name] * len(logs),
                                    "contract_address": [log['address'] for log in logs],
                                    "topic0": [log['topics'][0] for log in logs],
                                    "raw_data": [log['data'] for log in logs]
                                })
                                
                                logger.info(f"Created DataFrame for {event_name} with schema: {events_df.schema}")
                                logger.debug(f"Sample DataFrame rows for {event_name}:\n{events_df.head(2)}")
                                
                                # Convert to Arrow table with schema
                                events_table = pa.Table.from_pandas(events_df.to_pandas(), schema=EVENTS.to_arrow())
                                logger.info(f"Converted {event_name} DataFrame to Arrow table with {events_table.num_rows} rows")
                        else:
                            logger.warning(f"No 'result' field in response for {event_name}: {json.dumps(result, indent=2)}")

            # Log final statistics
            logger.info(f"\nFinal Data Statistics:")
            logger.info(f"Blocks: {len(blocks_df)} rows")
            logger.info(f"Events summary:")
            for name, table in events_data.items():
                logger.info(f"- {name}: {table.num_rows} rows")

            return Data(
                blocks=blocks_table,
                transactions=None,
                events=events_table
            )

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 