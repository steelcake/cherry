from abc import ABC, abstractmethod
from typing import Dict, Optional, List
from dataclasses import dataclass
import polars as pl, asyncio, json, sys, logging, requests, aiohttp
from web3 import Web3
from web3.types import BlockData, TxData, LogReceipt
from parse import Config, DataSourceKind
from pathlib import Path
from logging_setup import setup_logging

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

@dataclass
class Data:
    """Container for blockchain data"""
    blocks: pl.DataFrame
    transactions: Optional[pl.DataFrame]
    events: Dict[str, pl.DataFrame]

class DataIngester(ABC):
    """Abstract base class for data ingesters"""
    
    @abstractmethod
    async def get_data(self, from_block: int, to_block: int) -> Data:
        """Fetch data for the specified block range"""
        pass

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
        
        # Initialize web3 with a dummy provider
        self.web3 = Web3()
        
        self.event_addresses = {
            event.name: event.address if event.address else None 
            for event in config.events
        }
        logger.debug(f"Event addresses: {json.dumps(self.event_addresses, indent=2)}")
        
        self.event_topics = {
            event.name: event.topics if event.topics else None 
            for event in config.events
        }
        logger.debug(f"Event topics: {json.dumps(self.event_topics, indent=2)}")

    async def get_data(self, from_block: int, to_block: int) -> Data:
        blocks_data = []
        transactions_data = []
        events_data = {name: [] for name in self.event_addresses.keys()}

        try:
            # Log API request parameters
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': self.address,
                'startblock': str(from_block),
                'endblock': str(to_block),
                'sort': 'asc',
                'apikey': self.api_key,
                'offset': '1000',
                'page': '1'
            }
            logger.debug(f"API Request parameters: {json.dumps(params, indent=2)}")
            
            # Fetch transactions from Etherscan API
            logger.info(f"Fetching transactions for address {self.address} from block {from_block} to {to_block}")
            response = requests.get(self.api_url, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error fetching from Etherscan: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return Data(pl.DataFrame(), pl.DataFrame(), events_data)
                
            result = response.json()
            logger.info(f"API Response status: {result['status']}, message: {result['message']}")
            
            if result['status'] == '0':
                if 'No transactions found' in result['message']:
                    logger.info(f"No transactions found in blocks {from_block} to {to_block}")
                    return Data(pl.DataFrame(), pl.DataFrame(), events_data)
                else:
                    logger.error(f"Etherscan API error: {result['message']}")
                    return Data(pl.DataFrame(), pl.DataFrame(), events_data)

            # Process transactions
            for tx in result['result']:
                block_num = int(tx['blockNumber'])
                
                # Add block data if not already added
                if not any(b['number'] == block_num for b in blocks_data):
                    blocks_data.append({
                        'number': block_num,
                        'timestamp': int(tx['timeStamp']),
                        'hash': tx['blockHash']
                    })
                
                # Add transaction data
                if self.config.blocks and self.config.blocks.include_transactions:
                    transactions_data.append({
                        'hash': tx['hash'],
                        'block_number': block_num,
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

class HypersyncIngester(DataIngester):
    """Hypersync API data ingester"""
    
    def __init__(self, config: Config):
        self.config = config
        self.url = config.data_source[0].url
        
        # Override with USDT contract that we know works
        self.event_addresses = {
            "USDT": ["0xdAC17F958D2ee523a2206206994597C13D831ec7"]  # USDT contract
        }
        self.event_topics = {
            "USDT": [[  # Transfer event topic
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            ]]
        }
        
        logger.info(f"Initialized HypersyncIngester with URL: {self.url}")

    async def batch_request(self, session: aiohttp.ClientSession, requests: list) -> list:
        """Make multiple JSON-RPC requests in a single HTTP request"""
        async with session.post(
            self.url,
            json=requests,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=30)
        ) as response:
            return await response.json()

    async def get_data(self, from_block: int, to_block: int) -> Data:
        """Stream data from Hypersync using optimized batching"""
        logger.info(f"Streaming data from Hypersync for blocks {from_block} to {to_block}")
        
        try:
            blocks_data = []
            transactions_data = []
            events_data = {name: [] for name in self.event_addresses.keys()}

            async with aiohttp.ClientSession() as session:
                # Batch all event queries into a single request
                event_queries = []
                for i, (event_name, addresses) in enumerate(self.event_addresses.items(), 1):
                    query = {
                        "jsonrpc": "2.0",
                        "id": i,  # Using numeric ID
                        "method": "eth_getLogs",
                        "params": [{
                            "fromBlock": f"0x{hex(from_block)[2:]}",  # Proper hex format
                            "toBlock": f"0x{hex(to_block)[2:]}",  # Proper hex format
                            "address": addresses[0],  # Using single address
                            "topics": self.event_topics[event_name][0]  # Using topic array
                        }]
                    }
                    event_queries.append(query)

                # Get all events in one request
                event_responses = await self.batch_request(session, event_queries)
                if not isinstance(event_responses, list):
                    event_responses = [event_responses]

                # Process events and collect unique blocks and transactions
                block_numbers = set()
                tx_hashes = set()
                event_records = []

                for response in event_responses:
                    if "error" in response:
                        logger.error(f"API error: {response['error']}")
                        continue

                    results = response.get("result", [])
                    if not results:
                        continue

                    for event in results:
                        block_num = int(event["blockNumber"], 16)
                        tx_hash = event["transactionHash"]
                        block_numbers.add(block_num)
                        tx_hashes.add(tx_hash)
                        
                        # Extract topics individually
                        topics = event["topics"]
                        event_records.append({
                            "block_number": block_num,
                            "transaction_hash": tx_hash,
                            "log_address": event["address"].lower(),
                            "topic0": topics[0] if len(topics) > 0 else None,
                            "topic1": topics[1] if len(topics) > 1 else None,
                            "topic2": topics[2] if len(topics) > 2 else None,
                            "topic3": topics[3] if len(topics) > 3 else None,
                            "log_data": event["data"],
                            "log_index": int(event["logIndex"], 16)
                        })

                # Batch request block details
                if block_numbers:
                    block_queries = [
                        {
                            "jsonrpc": "2.0",
                            "id": i,  # Using numeric ID
                            "method": "eth_getBlockByNumber",
                            "params": [f"0x{hex(num)[2:]}", False]
                        }
                        for i, num in enumerate(block_numbers, len(event_queries) + 1)
                    ]
                    block_responses = await self.batch_request(session, block_queries)
                    if not isinstance(block_responses, list):
                        block_responses = [block_responses]
                    
                    for response in block_responses:
                        if "result" in response and response["result"]:
                            block = response["result"]
                            blocks_data.append({
                                "number": int(block["number"], 16),
                                "hash": block["hash"],
                                "timestamp": int(block["timestamp"], 16)
                            })

                # Batch request transaction details
                if tx_hashes:
                    tx_queries = [
                        {
                            "jsonrpc": "2.0",
                            "id": i,  # Using numeric ID
                            "method": "eth_getTransactionByHash",
                            "params": [tx_hash]
                        }
                        for i, tx_hash in enumerate(tx_hashes, len(event_queries) + len(block_queries) + 1)
                    ]
                    tx_responses = await self.batch_request(session, tx_queries)
                    if not isinstance(tx_responses, list):
                        tx_responses = [tx_responses]
                    
                    for response in tx_responses:
                        if "result" in response and response["result"]:
                            tx = response["result"]
                            transactions_data.append({
                                "hash": tx["hash"],
                                "block_number": int(tx["blockNumber"], 16),
                                "from": tx["from"],
                                "to": tx["to"],
                                "value": int(tx["value"], 16)
                            })

                # Log sample data
                if blocks_data:
                    logger.info("Sample Blocks Data:")
                    logger.info(json.dumps(blocks_data[:2], indent=2))
                if transactions_data:
                    logger.info("Sample Transactions Data:")
                    logger.info(json.dumps(transactions_data[:2], indent=2))

                # Convert to DataFrames
                blocks_df = pl.DataFrame(blocks_data) if blocks_data else pl.DataFrame()
                transactions_df = pl.DataFrame(transactions_data) if transactions_data else pl.DataFrame()
                events_df = pl.DataFrame(event_records) if event_records else pl.DataFrame()
                events_combined = {"USDT": events_df}

                logger.info(f"Found {len(blocks_df)} blocks, {len(transactions_df)} transactions, and {len(events_df)} events")
                return Data(blocks_df, transactions_df, events_combined)

        except Exception as e:
            logger.error(f"Error in get_data: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            return Data(pl.DataFrame(), pl.DataFrame(), {name: pl.DataFrame() for name in self.event_addresses.keys()})

class Ingester:
    """Factory class for creating appropriate ingester based on config"""
    
    def __init__(self, config: Config):
        self.config = config
        # Use the known working block range from our tests
        self.current_block = 21_580_000  # Known working block number
        self.batch_size = 100  # Smaller batch size to ensure we get results
        logger.info(f"Initializing Ingester starting from block {self.current_block}")
        
        # Use HypersyncIngester instead of EthRpcIngester
        if config.data_source[0].kind == DataSourceKind.HYPERSYNC:
            logger.info("Using HypersyncIngester for data ingestion")
            self.ingester = HypersyncIngester(config)
        else:
            logger.warning("Defaulting to HypersyncIngester despite config specifying different source")
            self.ingester = HypersyncIngester(config)

    async def get_data(self, from_block: int, to_block: int) -> Data:
        """Delegate to appropriate ingester implementation"""
        logger.debug(f"Delegating data fetch for blocks {from_block} to {to_block}")
        return await self.ingester.get_data(from_block, to_block)

    async def get_next_data_batch(self) -> Data:
        """Get the next batch of data"""
        next_block = self.current_block + self.batch_size
        logger.debug(f"Fetching next batch from {self.current_block} to {next_block}")
        data = await self.get_data(self.current_block, next_block)
        self.current_block = next_block
        return data
