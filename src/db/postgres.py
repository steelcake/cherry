from sqlalchemy import create_engine, text
import logging
import pyarrow as pa
import psycopg2
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

def stream_arrow_to_postgresql(connection, table: pa.Table, table_name: str, batch_size: int = 10000):
    """Stream Arrow table to PostgreSQL using record batches"""
    try:
        cursor = connection.cursor()
        
        # Get column names from schema
        column_names = table.schema.names
        
        # Generate placeholders for SQL
        placeholders = ','.join(['%s'] * len(column_names))
        insert_sql = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({placeholders})"
        
        # Create record batch reader from table
        reader = table.to_batches(max_chunksize=batch_size)
        
        batch_count = 0
        for batch in reader:
            try:
                # Convert batch to list of tuples for efficient insertion
                rows = zip(*[batch.column(i).to_pylist() for i in range(batch.num_columns)])
                
                # Execute batch insert
                cursor.executemany(insert_sql, rows)
                connection.commit()
                
                batch_count += 1
                logger.debug(f"Inserted batch {batch_count} into {table_name}")
                
            except Exception as e:
                logger.error(f"Error processing batch {batch_count}: {e}")
                connection.rollback()
                raise
                
        logger.info(f"Successfully streamed {batch_count} batches to {table_name}")
        
    except Exception as e:
        logger.error(f"Error streaming to {table_name}: {e}")
        raise
    finally:
        cursor.close()

def ingest_data(engine, data: Data):
    """Ingest data to PostgreSQL using Arrow streaming"""
    try:
        # Get raw psycopg2 connection from SQLAlchemy engine
        connection = engine.raw_connection()
        
        # Stream blocks to PostgreSQL
        if data.blocks and data.blocks.num_rows > 0:
            logger.info(f"Streaming {data.blocks.num_rows} blocks to PostgreSQL")
            stream_arrow_to_postgresql(connection, data.blocks, "blocks")
            
        # Stream events to PostgreSQL
        for event_name, event_table in data.events.items():
            if event_table.num_rows > 0:
                logger.info(f"Streaming {event_table.num_rows} {event_name} events to PostgreSQL")
                stream_arrow_to_postgresql(connection, event_table, "events")
                
        connection.close()
                
    except Exception as e:
        logger.error(f"Error ingesting data to PostgreSQL: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise