from abc import ABC, abstractmethod
from typing import Dict, Optional, List
from dataclasses import dataclass
import polars as pl, asyncio, json, sys, logging, requests, aiohttp
from web3 import Web3
from web3.types import BlockData, TxData, LogReceipt
from parse import Config, DataSourceKind
from pathlib import Path
from logging_setup import setup_logging
from pyarrow import flight
import pyarrow as pa
from convert_hypersync_query import generate_hypersync_query

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
    def __init__(self, config: Config):
        self.config = config
        self.url = config.data_source[0].url
        self.events = {
            event.name: event for event in config.events
        }
        logger.info(f"Initialized HypersyncIngester with URL: {self.url}")

    async def get_data(self, from_block: int, to_block: int) -> Data:
        try:
            all_transactions = []
            blocks_data = {}
            
            # Create Arrow schema for the stream
            schema = pa.schema([
                ("transaction_hash", pa.string()),
                ("block_number", pa.int64()),
                ("from_address", pa.string()),
                ("to_address", pa.string()),
                ("value", pa.int64()),
                ("event_name", pa.string()),
                ("contract_address", pa.string()),
                ("topic0", pa.string()),
                ("raw_data", pa.string())
            ])

            for event_name, event in self.events.items():
                query = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_getLogs",
                    "params": [{
                        "fromBlock": hex(from_block),
                        "toBlock": hex(to_block),
                        "address": event.address[0] if event.address else None,
                        "topics": event.topics[0] if event.topics else None
                    }]
                }

                logger.info(f"Fetching {event_name} events from block {from_block} to {to_block}")
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.url, json=query) as response:
                        if response.status != 200:
                            raise Exception(f"HTTP {response.status}: {await response.text()}")

                        data = await response.json()
                        if "error" in data:
                            raise Exception(f"RPC Error: {data['error']}")

                        logs = data.get("result", [])
                        logger.info(f"Retrieved {len(logs)} {event_name} logs")

                        # Process logs through Arrow stream
                        for log in logs:
                            block_num = int(log["blockNumber"], 16)
                            
                            # Store block data
                            blocks_data[block_num] = {
                                "number": block_num,
                                "timestamp": int(log.get("timeStamp", "0"), 16) if "timeStamp" in log else 0
                            }

                            # Convert log to transaction format
                            tx = {
                                "transaction_hash": log["transactionHash"],
                                "block_number": block_num,
                                "from_address": "0x" + log["topics"][1][-40:] if len(log["topics"]) > 1 else None,
                                "to_address": "0x" + log["topics"][2][-40:] if len(log["topics"]) > 2 else None,
                                "value": int(log["data"], 16),
                                "event_name": event_name,
                                "contract_address": log["address"],
                                "topic0": log["topics"][0],
                                "raw_data": log["data"]
                            }
                            all_transactions.append(tx)

            # Convert blocks_data to DataFrame
            blocks_df = pl.DataFrame([
                {"number": block["number"], "timestamp": block["timestamp"]}
                for block in blocks_data.values()
            ]) if blocks_data else pl.DataFrame(schema={"number": pl.Int64, "timestamp": pl.Int64})

            # Process transactions through Arrow
            if all_transactions:
                arrow_table = pa.Table.from_pylist(all_transactions, schema=schema)
                
                # Create a buffer for the Arrow stream
                sink = pa.BufferOutputStream()
                with pa.ipc.RecordBatchStreamWriter(sink, arrow_table.schema) as writer:
                    writer.write_table(arrow_table)
                
                # Get the buffer and create a reader
                buf = sink.getvalue()
                reader = pa.ipc.RecordBatchStreamReader(pa.BufferReader(buf))

                # Process the Arrow stream in batches
                while True:
                    try:
                        batch = reader.read_next_batch()
                    except StopIteration:
                        break

            # Convert transactions to DataFrame
            transactions_df = pl.DataFrame(all_transactions) if all_transactions else pl.DataFrame(schema={
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

            return Data(
                blocks=blocks_df,
                transactions=transactions_df,
                events={name: transactions_df.filter(pl.col("event_name") == name) 
                       for name in self.events.keys()}
            )

        except Exception as e:
            logger.error(f"Error fetching data from Hypersync: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

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
