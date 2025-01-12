from ingester import Ingester, Data
from datetime import datetime
from pathlib import Path
import polars as pl, logging, sys, json, asyncio
from sqlalchemy import create_engine, text
from parse import parse_config
from logging_setup import setup_logging
from sqlalchemy.types import BigInteger

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def create_tables(engine):
    """Create necessary tables if they don't exist"""
    logger.info("Creating database tables if they don't exist")
    try:
        with engine.connect() as conn:
            # Create blocks table
            logger.debug("Creating blocks table")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS blocks (
                    number BIGINT,
                    timestamp BIGINT
                )
            """))
            
            # Create transactions table
            logger.debug("Creating transactions table")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_hash VARCHAR(66),
                    block_number BIGINT,
                    from_address VARCHAR(42),
                    to_address VARCHAR(42),
                    value NUMERIC(78,0)
                )
            """))
            
            # Create events table
            logger.debug("Creating events table")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL,
                    transaction_hash VARCHAR(66),
                    block_number BIGINT,
                    from_address VARCHAR(42),
                    to_address VARCHAR(42),
                    value NUMERIC(78,0),
                    event_name VARCHAR(100),
                    contract_address VARCHAR(42),
                    topic0 VARCHAR(66),
                    raw_data TEXT
                )
            """))
            
            conn.commit()
            logger.info("Successfully created all database tables")
            
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise

def ingest_data(engine, data: Data):
    """Ingest data into PostgreSQL database"""
    try:
        # Ingest blocks
        if len(data.blocks) > 0:
            logger.info(f"Ingesting {len(data.blocks)} blocks")
            # Create temporary table for blocks
            temp_blocks = data.blocks.unique(subset=["number"]).to_pandas()
            
            # Use SQLAlchemy to handle conflicts
            from sqlalchemy.dialects.postgresql import insert
            from sqlalchemy import Table, MetaData, Column, BigInteger
            
            # Define the blocks table
            metadata = MetaData()
            blocks_table = Table(
                'blocks', 
                metadata,
                Column('number', BigInteger, primary_key=True),
                Column('timestamp', BigInteger)
            )
            
            with engine.connect() as conn:
                # Convert DataFrame to list of dictionaries
                block_records = temp_blocks.to_dict('records')
                
                # Create insert statement with ON CONFLICT
                insert_stmt = insert(blocks_table).values(block_records)
                
                # Execute the statement
                conn.execute(insert_stmt)
                conn.commit()
                
            logger.info("Successfully ingested blocks")

        # Ingest transactions
        if len(data.transactions) > 0:
            logger.info(f"Ingesting {len(data.transactions)} transactions")
            try:
                # Remove duplicates and select required columns
                unique_txs = data.transactions.unique(subset=["transaction_hash"]).select([
                    "transaction_hash",
                    "block_number",
                    "from_address",
                    "to_address",
                    "value"
                ])
                
                # Convert to pandas and use to_sql
                unique_txs.to_pandas().to_sql(
                    name="transactions",
                    con=engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                logger.info("Successfully ingested transactions")
            except Exception as e:
                if "duplicate key value" in str(e):
                    logger.warning("Skipping duplicate transaction records")
                else:
                    raise

        # Ingest events
        for event_name, events_df in data.events.items():
            if len(events_df) > 0:
                logger.info(f"Processing events for {event_name}")
                try:
                    # Remove duplicates based on transaction hash and log index
                    unique_events = events_df.unique(
                        subset=["transaction_hash", "block_number", "contract_address", "topic0"]
                    ).select([
                        "transaction_hash",
                        "block_number",
                        "from_address",
                        "to_address",
                        "value",
                        "event_name",
                        "contract_address",
                        "topic0",
                        "raw_data"
                    ])
                    
                    # Convert to pandas and use to_sql
                    unique_events.to_pandas().to_sql(
                        name="events",
                        con=engine,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    logger.info(f"Successfully ingested {len(unique_events)} events for {event_name}")
                except Exception as e:
                    if "duplicate key value" in str(e):
                        logger.warning(f"Skipping duplicate event records for {event_name}")
                    else:
                        raise

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
        ingest_data(engine, data)
        
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