from sqlalchemy import create_engine, text, Table, MetaData, Column, BigInteger
from sqlalchemy.dialects.postgresql import insert
import logging
from src.schemas.database_schemas import (
    BLOCKS_TABLE_SQL, 
    TRANSACTIONS_TABLE_SQL, 
    EVENTS_TABLE_SQL
)
from src.ingesters.base import Data

logger = logging.getLogger(__name__)

def create_engine_from_config(url: str):
    """Create SQLAlchemy engine from config URL"""
    return create_engine(url)

def create_tables(engine):
    """Create necessary tables if they don't exist"""
    logger.info("Creating database tables if they don't exist")
    try:
        with engine.connect() as conn:
            logger.debug("Creating blocks table")
            conn.execute(text(BLOCKS_TABLE_SQL))
            
            logger.debug("Creating transactions table")
            conn.execute(text(TRANSACTIONS_TABLE_SQL))
            
            logger.debug("Creating events table")
            conn.execute(text(EVENTS_TABLE_SQL))
            
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
            temp_blocks = data.blocks.unique(subset=["number"]).to_pandas()
            
            # Define the blocks table
            metadata = MetaData()
            blocks_table = Table(
                'blocks', 
                metadata,
                Column('number', BigInteger, primary_key=True),
                Column('timestamp', BigInteger)
            )
            
            with engine.connect() as conn:
                block_records = temp_blocks.to_dict('records')
                insert_stmt = insert(blocks_table).values(block_records)
                conn.execute(insert_stmt)
                conn.commit()
                
            logger.info("Successfully ingested blocks")

        # Ingest transactions
        if len(data.transactions) > 0:
            logger.info(f"Ingesting {len(data.transactions)} transactions")
            try:
                unique_txs = data.transactions.unique(subset=["transaction_hash"]).select([
                    "transaction_hash",
                    "block_number",
                    "from_address",
                    "to_address",
                    "value"
                ])
                
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