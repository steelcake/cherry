import json, logging, aiohttp
import polars as pl
import pyarrow as pa
import cryo
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config
from src.utils.logging_setup import setup_logging

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
            # Fetch blocks using cryo collect
            blocks_range = [f"{from_block}:{to_block}"]
            blocks_data = await cryo.async_collect(
                "blocks",
                blocks=blocks_range,
                rpc=self.url,
                output_format="pandas",
                hex=True
            )

            # Convert numeric columns to Int64 in pandas first
            numeric_columns = ['block_number', 'gas_used', 'timestamp', 'base_fee_per_gas', 'chain_id']
            for col in numeric_columns:
                if col in blocks_data.columns:
                    blocks_data[col] = blocks_data[col].astype('int64')

            # Convert blocks DataFrame to Polars with all available columns
            blocks_df = pl.from_pandas(blocks_data[[
                'block_hash',
                'author',
                'block_number',
                'gas_used',
                'extra_data',
                'timestamp',
                'base_fee_per_gas',
                'chain_id'
            ]])
            
            # Initialize empty transactions DataFrame with schema
            transactions_df = pl.DataFrame(schema={
                "transaction_hash": pl.Utf8,
                "block_number": pl.Int64,
                "from_address": pl.Utf8,
                "to_address": pl.Utf8,
                "value": pl.Int64,
                "event_name": pl.Utf8,
                "contract_address": pl.Utf8,
                "topic0": pl.Utf8,
                "raw_data": pl.Utf8
            })
            
            # Fetch events for each configured event type
            events_data = {}
            for event_name, event in self.events.items():
                # Fetch logs using cryo
                logs_data = await cryo.async_collect(
                    "logs",
                    blocks=blocks_range,
                    rpc=self.url,
                    address=event.address,
                    topic0=[event.topics[0][0]],  # Pass topic0 as a list
                    output_format="pandas",
                    hex=True
                )
                
                if not logs_data.empty:
                    # Convert block_number to Int64 in logs data
                    if 'block_number' in logs_data.columns:
                        logs_data['block_number'] = logs_data['block_number'].astype('int64')

                    # Convert logs to our transaction format
                    event_df = pl.DataFrame({
                        "transaction_hash": logs_data['transaction_hash'],
                        "block_number": logs_data['block_number'],
                        "from_address": logs_data['topic1'].apply(lambda x: '0x' + x[-40:] if x else None),
                        "to_address": logs_data['topic2'].apply(lambda x: '0x' + x[-40:] if x else None),
                        "value": logs_data['data'].apply(lambda x: int(x, 16) if x else 0),
                        "event_name": event_name,
                        "contract_address": logs_data['address'],
                        "topic0": logs_data['topic0'],
                        "raw_data": logs_data['data']
                    })
                    
                    # Store event data
                    events_data[event_name] = event_df
                    
                    # Append to transactions
                    transactions_df = pl.concat([transactions_df, event_df])
            
            # Log statistics
            logger.info(f"\nDataFrame Statistics:")
            logger.info(f"Blocks DataFrame: {len(blocks_df)} rows")
            logger.info(f"Transactions DataFrame: {len(transactions_df)} rows")
            logger.info(f"Events DataFrame: {len(event_df)} rows")

            if len(event_df) > 0:
                logger.debug(f"Events DataFrame Schema: {event_df.schema}")
                logger.debug(f"Sample Events DataFrame:\n{event_df.head(2)}")
            
            if len(blocks_df) > 0:
                logger.debug(f"Blocks DataFrame Schema: {blocks_df.schema}")
                logger.debug(f"Sample Blocks DataFrame:\n{blocks_df.head(2)}")
            
            if len(transactions_df) > 0:
                logger.debug(f"Transactions DataFrame Schema: {transactions_df.schema}")
                logger.debug(f"Sample Transactions DataFrame:\n{transactions_df.head(2)}")
            

            return Data(
                blocks=blocks_df,
                transactions=transactions_df,
                events=events_data
            )

        except Exception as e:
            logger.error(f"Error fetching data from Hypersync: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 