from abc import ABC, abstractmethod
from typing import Dict, Optional
from dataclasses import dataclass
import polars as pl
import asyncio
from pathlib import Path
from web3 import Web3
from web3.types import BlockData, TxData, LogReceipt
from parse import Config, DataSourceKind
import json
import sys
import requests

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
        # Parse Etherscan API URL components
        self.api_url = config.data_source[0].url.split('?')[0]
        self.api_params = dict(param.split('=') for param in config.data_source[0].url.split('?')[1].split('&'))
        self.api_key = self.api_params.get('apikey')
        self.address = self.api_params.get('address')
        
        # Initialize web3 with a dummy provider (we'll use requests directly)
        self.web3 = Web3()
        
        self.event_addresses = {
            event.name: event.address if event.address else None 
            for event in config.events
        }
        self.event_topics = {
            event.name: event.topics if event.topics else None 
            for event in config.events
        }

    async def get_data(self, from_block: int, to_block: int) -> Data:
        blocks_data = []
        transactions_data = []
        events_data = {name: [] for name in self.event_addresses.keys()}

        try:
            # Fetch transactions from Etherscan API
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': self.address,
                'startblock': str(from_block),
                'endblock': str(to_block),
                'sort': 'asc',
                'apikey': self.api_key,
                'offset': '1000',  # Number of transactions per request
                'page': '1'
            }
            
            print(f"Fetching transactions for address {self.address} from block {from_block} to {to_block}")
            response = requests.get(self.api_url, params=params)
            
            if response.status_code != 200:
                print(f"Error fetching from Etherscan: {response.status_code}")
                print(f"Response: {response.text}")
                return Data(pl.DataFrame(), pl.DataFrame(), events_data)
                
            result = response.json()
            print(f"API Response status: {result['status']}, message: {result['message']}")
            
            if result['status'] == '0':
                if 'No transactions found' in result['message']:
                    print(f"No transactions found in blocks {from_block} to {to_block}")
                    # Return empty data but don't treat as error
                    return Data(pl.DataFrame(), pl.DataFrame(), events_data)
                else:
                    print(f"Etherscan API error: {result['message']}")
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

            # Convert to DataFrames
            blocks_df = pl.DataFrame(blocks_data) if blocks_data else pl.DataFrame()
            transactions_df = pl.DataFrame(transactions_data) if transactions_data else pl.DataFrame()
            
            print(f"Found {len(blocks_data)} blocks and {len(transactions_data)} transactions")
            return Data(
                blocks=blocks_df,
                transactions=transactions_df,
                events=events_data
            )
            
        except Exception as e:
            print(f"Error processing data: {e}")
            print(f"Error occurred at line {e.__traceback__.tb_lineno}")
            return Data(pl.DataFrame(), pl.DataFrame(), events_data)

class Ingester:
    """Factory class for creating appropriate ingester based on config"""
    
    def __init__(self, config: Config):
        self.config = config
        self.current_block = 4634748  # USDT contract deployment block
        self.batch_size = 1000  # Increased batch size for larger ranges
        
        # For now, we only support ETH RPC
        self.ingester = EthRpcIngester(config)

    async def get_data(self, from_block: int, to_block: int) -> Data:
        """Delegate to appropriate ingester implementation"""
        return await self.ingester.get_data(from_block, to_block)

    async def get_next_data_batch(self) -> Data:
        """Get the next batch of data"""
        next_block = self.current_block + self.batch_size
        data = await self.get_data(self.current_block, next_block)
        self.current_block = next_block
        return data
