from ingester import Ingester, Data
from parse import parse_config
from pathlib import Path
import polars as pl
from sqlalchemy import create_engine, text
import asyncio
from datetime import datetime

def create_tables(engine):
    """Create necessary tables if they don't exist"""
    with engine.connect() as conn:
        # Blocks table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS blocks (
                number BIGINT PRIMARY KEY,
                timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT blocks_unique UNIQUE (number)
            )
        """))
        
        # Transactions table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS transactions (
                hash VARCHAR(66) PRIMARY KEY,
                block_number BIGINT,
                from_address VARCHAR(42),
                to_address VARCHAR(42),
                value NUMERIC(78,0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (block_number) REFERENCES blocks(number),
                CONSTRAINT tx_unique UNIQUE (hash)
            )
        """))
        
        # Events table
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (block_number) REFERENCES blocks(number),
                FOREIGN KEY (transaction_hash) REFERENCES transactions(hash),
                CONSTRAINT events_unique UNIQUE (transaction_hash, topic0, topic1, topic2, topic3)
            )
        """))
        
        conn.commit()

def ingest_data(data: Data, engine):
    """Ingest data into PostgreSQL"""
    
    # Ingest blocks
    if len(data.blocks) > 0:
        blocks_df = data.blocks.select([
            pl.col("number"),
            pl.col("timestamp").cast(pl.Datetime)
        ])
        # Convert to pandas using default method
        blocks_df.to_pandas().to_sql(
            name="blocks",
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
    
    # Ingest transactions
    if data.transactions is not None and len(data.transactions) > 0:
        tx_df = data.transactions.select([
            pl.col("hash"),
            pl.col("block_number"),
            pl.col("from").alias("from_address"),
            pl.col("to").alias("to_address"),
            pl.col("value")
        ])
        # Convert to pandas using default method
        tx_df.to_pandas().to_sql(
            name="transactions",
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
    
    # Ingest events
    for event_name, events_df in data.events.items():
        if len(events_df) > 0:
            events_df = events_df.with_columns([
                pl.lit(event_name).alias("name"),
                pl.col("topics").list.get(0).alias("topic0"),
                pl.col("topics").list.get(1).alias("topic1"),
                pl.col("topics").list.get(2).alias("topic2"),
                pl.col("topics").list.get(3).alias("topic3"),
            ])
            # Convert to pandas using default method
            events_df.to_pandas().to_sql(
                name="events",
                con=engine,
                if_exists='append',
                index=False,
                method='multi'
            )

async def process_batch(ingester: Ingester, engine) -> bool:
    """Process a single batch of data"""
    print(f"Processing blocks {ingester.current_block} to {ingester.current_block + ingester.batch_size}")
    
    # Get next batch of data
    data = await ingester.get_next_data_batch()
    
    # If no more data, return False
    if len(data.blocks) == 0:
        return False
        
    # Ingest to PostgreSQL
    ingest_data(data, engine)
    
    print(f"Processed {len(data.blocks)} blocks")
    return True

async def main():
    # Load config
    config = parse_config(Path("config.yaml"))
    
    # Create database engine
    engine = create_engine(config.output[0].url)
    
    # Create tables
    create_tables(engine)
    
    # Initialize ingester
    ingester = Ingester(config)
    
    # Process data in batches
    while True:
        try:
            has_more_data = await process_batch(ingester, engine)
            if not has_more_data:
                break
        except Exception as e:
            print(f"Error processing batch: {e}")
            break

if __name__ == "__main__":
    asyncio.run(main()) 