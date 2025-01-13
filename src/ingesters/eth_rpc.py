import logging
import json
import cryo
import polars as pl
from src.ingesters.base import DataIngester, Data
from src.config.parser import Config

logger = logging.getLogger(__name__)

class EthRpcIngester(DataIngester):
    def __init__(self, config: Config):
        self.config = config
        logger.debug("Initializing EthRpcIngester")
        # Parse Etherscan API URL components
        self.api_url = config.data_source[0].url.split('?')[0]
        self.api_params = dict(param.split('=') for param in config.data_source[0].url.split('?')[1].split('&'))
        logger.debug(f"API Parameters: {json.dumps(self.api_params, indent=2)}")
        
        self.api_key = self.api_params.get('apikey')
        self.address = self.api_params.get('address')
        
        # Initialize cryo provider
        self.provider = cryo.JsonRpc(self.api_url)
        
        self.event_addresses = {event.name: event.address for event in config.events}
        self.event_topics = {event.name: event.topics for event in config.events}
        logger.debug(f"Event addresses: {json.dumps(self.event_addresses, indent=2)}")
        logger.debug(f"Event topics: {json.dumps(self.event_topics, indent=2)}")

    async def get_data(self, from_block: int, to_block: int) -> Data:
        blocks_data = []
        transactions_data = []
        events_data = {name: [] for name in self.event_addresses.keys()}

        try:
            # Use cryo to fetch block data
            blocks = await self.provider.get_blocks(
                start_block=from_block,
                end_block=to_block,
                include_transactions=True
            )
            
            for block in blocks:
                blocks_data.append({
                    'block_number': block['number'],
                    'timestamp': block['timestamp'],
                    'hash': block['hash']
                })
                
                if self.config.blocks and self.config.blocks.include_transactions:
                    for tx in block['transactions']:
                        transactions_data.append({
                            'hash': tx['hash'],
                            'block_number': block['number'],
                            'from': tx['from'],
                            'to': tx['to'],
                            'value': int(tx['value'])
                        })

            # Log processed data
            if blocks_data:
                logger.debug(f"Sample blocks data: {json.dumps(blocks_data[:2], indent=2)}")
            if transactions_data:
                logger.debug(f"Sample transactions data: {json.dumps(transactions_data[:2], indent=2)}")
            
            # Convert to DataFrames
            blocks_df = pl.DataFrame(blocks_data) if blocks_data else pl.DataFrame()
            transactions_df = pl.DataFrame(transactions_data) if transactions_data else pl.DataFrame()
            
            # Log DataFrame information
            logger.debug(f"Blocks DataFrame Schema: {blocks_df.schema}")
            if len(blocks_df) > 0:
                logger.debug(f"Sample Blocks DataFrame:\n{blocks_df.head(2)}")
            
            logger.debug(f"Transactions DataFrame Schema: {transactions_df.schema}")
            if len(transactions_df) > 0:
                logger.debug(f"Sample Transactions DataFrame:\n{transactions_df.head(2)}")
            
            logger.info(f"Found {len(blocks_data)} blocks and {len(transactions_data)} transactions")
            return Data(
                blocks=blocks_df,
                transactions=transactions_df,
                events=events_data
            )
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            return Data(pl.DataFrame(), pl.DataFrame(), events_data) 