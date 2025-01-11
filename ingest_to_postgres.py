from ingester import Ingester, Data
from parse import parse_config
from pathlib import Path
import polars as pl
from sqlalchemy import create_engine, text
import asyncio
from datetime import datetime
import logging
import sys
from logging_setup import setup_logging
import json

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def create_tables(engine):
    """Create necessary tables if they don't exist"""
    logger.info("Creating database tables if they don't exist")
    try:
        with engine.connect() as conn:
            # Blocks table
            logger.debug("Creating blocks table")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS blocks (
                    number BIGINT PRIMARY KEY,
                    timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    --CONSTRAINT blocks_unique UNIQUE (number)
                )
            """))
            
            # Transactions table
            logger.debug("Creating transactions table")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS transactions (
                    hash VARCHAR(66) PRIMARY KEY,
                    block_number BIGINT,
                    from_address VARCHAR(42),
                    to_address VARCHAR(42),
                    value NUMERIC(78,0),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    --FOREIGN KEY (block_number) REFERENCES blocks(number),
                    --CONSTRAINT tx_unique UNIQUE (hash)
                )
            """))
            
            # Events table - Updated schema to match DataFrame columns
            logger.debug("Creating events table")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    block_number BIGINT,
                    transaction_hash VARCHAR(66),
                    address VARCHAR(42),
                    topic0 VARCHAR(66),
                    topic1 VARCHAR(66),
                    topic2 VARCHAR(66),
                    topic3 VARCHAR(66),
                    data TEXT,
                    log_index INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    --FOREIGN KEY (block_number) REFERENCES blocks(number),
                    --FOREIGN KEY (transaction_hash) REFERENCES transactions(hash),
                    --CONSTRAINT events_unique UNIQUE (transaction_hash, log_index)
                )
            """))
            
            conn.commit()
            logger.info("Successfully created all database tables")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise

def ingest_data(data: Data, engine):
    """Ingest data into PostgreSQL"""
    try:
        # Log detailed data statistics
        logger.info("=== Starting Data Ingestion ===")
        logger.info(f"Total blocks to ingest: {len(data.blocks)}")
        logger.info(f"Total transactions to ingest: {len(data.transactions) if data.transactions is not None else 0}")
        logger.info(f"Events to ingest: {', '.join(f'{name}: {len(df)}' for name, df in data.events.items())}")
        
        # Extract unique transaction hashes from events if no transactions provided
        if data.transactions is None or len(data.transactions) == 0:
            logger.info("No transactions provided, extracting from events")
            tx_hashes = set()
            block_numbers = {}  # Map tx_hash to block_number
            
            for events_df in data.events.values():
                # Get unique transaction hashes and their block numbers
                tx_data = events_df.select([
                    "transaction_hash",
                    "block_number"
                ]).unique(subset=["transaction_hash"])
                
                for tx_hash, block_num in zip(tx_data["transaction_hash"], tx_data["block_number"]):
                    tx_hashes.add(tx_hash)
                    block_numbers[tx_hash] = block_num
            
            # Create transactions DataFrame
            data.transactions = pl.DataFrame({
                "hash": list(tx_hashes),
                "block_number": [block_numbers[tx_hash] for tx_hash in tx_hashes],
                "from": [""] * len(tx_hashes),  # Empty placeholder
                "to": [""] * len(tx_hashes),    # Empty placeholder
                "value": [0] * len(tx_hashes)   # Zero value placeholder
            })
            logger.info(f"Created {len(tx_hashes)} placeholder transactions from events")

        # First ingest blocks
        if len(data.blocks) > 0:
            logger.info(f"Ingesting {len(data.blocks)} blocks")
            blocks_df = data.blocks.select([
                pl.col("number"),
                pl.col("timestamp").cast(pl.Datetime)
            ])
            try:
                blocks_df.to_pandas().to_sql(
                    name="blocks",
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                logger.info(f"Successfully ingested {len(blocks_df)} blocks")
            except Exception as e:
                if "duplicate key value" in str(e):
                    logger.warning("Skipping duplicate block records")
                else:
                    raise

        # Then ingest transactions
        if len(data.transactions) > 0:
            logger.info(f"Ingesting {len(data.transactions)} transactions")
            tx_df = data.transactions.select([
                pl.col("hash"),
                pl.col("block_number"),
                pl.col("from").alias("from_address"),
                pl.col("to").alias("to_address"),
                pl.col("value")
            ])
            try:
                tx_df.to_pandas().to_sql(
                    name="transactions",
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                logger.info(f"Successfully ingested {len(tx_df)} transactions")
            except Exception as e:
                if "duplicate key value" in str(e):
                    logger.warning("Skipping duplicate transaction records")
                else:
                    raise

        # Ingest events
        for event_name, events_df in data.events.items():
            if len(events_df) > 0:
                logger.info(f"Processing events for {event_name}")
                logger.debug(f"Event data sample: {events_df.head(1).to_dict()}")
                
                # Add event name column and rename columns to match DB schema
                events_df = events_df.with_columns([
                    pl.lit(event_name).alias("name")
                ]).rename({
                    "log_address": "address",
                    "log_data": "data"
                })
                
                # Select required columns in correct order
                events_df = events_df.select([
                    "name",
                    "block_number",
                    "transaction_hash",
                    "address",
                    "topic0",
                    "topic1", 
                    "topic2",
                    "topic3",
                    "data",
                    "log_index"
                ])
                
                try:
                    events_df.to_pandas().to_sql(
                        name="events",
                        con=engine,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    logger.info(f"Successfully ingested {len(events_df)} events for {event_name}")
                except Exception as e:
                    if "duplicate key value" in str(e):
                        logger.warning(f"Skipping duplicate event records for {event_name}")
                    else:
                        logger.error(f"Error ingesting data to PostgreSQL: {e}")
                        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
                        raise
        
        logger.info("=== Data Ingestion Completed ===")
    except Exception as e:
        logger.error(f"Error ingesting data to PostgreSQL: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise

async def process_batch(ingester: Ingester, engine) -> bool:
    """Process a single batch of data"""
    current_block = ingester.current_block
    next_block = current_block + ingester.batch_size
    logger.info(f"=== Processing Batch: Blocks {current_block} to {next_block} ===")
    
    try:
        # Get next batch of data
        logger.info("Fetching next batch of data...")
        data = await ingester.get_next_data_batch()
        logger.info(f"Received data batch with {len(data.blocks)} blocks")
        
        # If no more data, return False
        if len(data.blocks) == 0:
            logger.info("No more blocks to process in this batch")
            return False
        
        # Log batch statistics
        logger.info("Batch Statistics:")
        logger.info(f"- Blocks: {len(data.blocks)}")
        logger.info(f"- Transactions: {len(data.transactions) if data.transactions is not None else 0}")
        logger.info(f"- Events: {sum(len(df) for df in data.events.values())}")
            
        # Ingest to PostgreSQL
        logger.info("Starting PostgreSQL ingestion...")
        ingest_data(data, engine)
        
        logger.info(f"Successfully processed batch of {len(data.blocks)} blocks")
        logger.info("=== Batch Processing Completed ===")
        return True
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise

async def main():
    logger.info("Starting blockchain data ingestion")
    try:
        # Load config
        logger.info("Loading configuration")
        config = parse_config(Path("config.yaml"))
        
        # Create database engine
        logger.info("Creating database engine")
        engine = create_engine(config.output[0].url)
        
        # Create tables
        create_tables(engine)
        
        # Initialize ingester
        logger.info("Initializing data ingester")
        ingester = Ingester(config)
        
        # Process data in batches
        logger.info("Starting batch processing")
        while True:
            try:
                has_more_data = await process_batch(ingester, engine)
                if not has_more_data:
                    logger.info("Completed processing all blocks")
                    break
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                break
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise
    finally:
        logger.info("Blockchain data ingestion completed")

if __name__ == "__main__":
    asyncio.run(main()) 