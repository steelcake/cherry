import logging
import asyncio
from typing import Optional, Dict, List
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from src.writers.base import DataWriter
from src.types.data import Data
from src.schemas.blockchain_schemas import BLOCKS, EVENTS, TRANSACTIONS
from src.schemas.base import SchemaConverter
import polars as pl

logger = logging.getLogger(__name__)

class ClickHouseWriter(DataWriter):
    """Writer for writing data to ClickHouse"""
    # Base event table definition template
    EVENT_TABLE_TEMPLATE = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            removed Bool,
            log_index Int64,
            transaction_index Int64,
            transaction_hash String,
            block_hash String,
            block_number Int64,
            address String,
            data String,
            topic0 String,
            topic1 Nullable(String),
            topic2 Nullable(String),
            topic3 Nullable(String),
            block_timestamp UInt64,
            INDEX address_idx address TYPE bloom_filter GRANULARITY 1
        ) ENGINE = MergeTree
        PRIMARY KEY (block_number, log_index)
        ORDER BY (block_number, log_index, address)
    """

    def __init__(self, host: str, port: int = 8123, username: str = 'default',
                 password: str = '', database: str = 'default', secure: bool = False):
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.secure = secure
        
        # Initialize client
        self.client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
            secure=self.secure
        )
        logger.info(f"Initialized ClickHouseWriter with host {host}:{port}, database {database}")
        
        # Create tables if they don't exist
        self.create_tables()
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        logger.info("Creating ClickHouse tables if they don't exist")
        try:
            # Create blocks table
            self.client.command("""
                CREATE TABLE IF NOT EXISTS blocks (
                    block_number Int64,
                    block_timestamp UInt64,
                    PRIMARY KEY (block_number)
                ) ENGINE = ReplacingMergeTree
            """)
            
            logger.info("Successfully created ClickHouse tables")
            
        except Exception as e:
            logger.error(f"Error creating ClickHouse tables: {e}")
            raise

    async def write(self, data: Data) -> None:
        """Write data to ClickHouse"""
        try:
            # Prepare and validate data using base class method
            blocks_df, events_dict = self.prepare_data(data)
            
            # Write blocks
            if blocks_df is not None:
                logger.info(f"Writing {blocks_df.height} blocks to ClickHouse")
                await asyncio.to_thread(
                    self.client.insert_df,
                    'blocks',
                    blocks_df.to_pandas()
                )
            
            # Write events
            total_events = 0
            for event_name, event_df in events_dict.items():
                if event_df.height > 0:
                    # Create event-specific table if it doesn't exist
                    table_name = f"{event_name.lower()}_events"
                    self.client.command(self.EVENT_TABLE_TEMPLATE.format(table_name=table_name))
                    
                    logger.info(f"Writing {event_df.height} {event_name} events to ClickHouse")
                    await asyncio.to_thread(
                        self.client.insert_df,
                        table_name,
                        event_df.to_pandas()
                    )
                    total_events += event_df.height
            
            if total_events > 0:
                logger.info(f"Successfully wrote {total_events} total events to ClickHouse")
                
        except Exception as e:
            logger.error(f"Error writing to ClickHouse: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 