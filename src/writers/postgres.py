from sqlalchemy import Engine, text
import logging
import pyarrow as pa
import psycopg2
from src.schemas.blockchain_schemas import BLOCKS, TRANSACTIONS, EVENTS
from src.schemas.base import SchemaConverter
from src.types.data import Data
from src.writers.base import DataWriter
import polars as pl
from typing import Tuple, Optional, Dict
import asyncio

logger = logging.getLogger(__name__)

class PostgresWriter(DataWriter):
    def __init__(self, engine: Engine):
        self.engine = engine
        # Extract connection parameters from SQLAlchemy engine
        self.conn_params = {
            'host': engine.url.host,
            'port': engine.url.port,
            'database': engine.url.database,
            'user': engine.url.username,
            'password': engine.url.password
        }
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        logger.info("Creating database tables if they don't exist")
        try:
            with self.engine.connect() as conn:
                logger.debug("Creating blocks table")
                conn.execute(text(SchemaConverter.to_sql(BLOCKS, "blocks")))
                
                logger.debug("Creating transactions table")
                conn.execute(text(SchemaConverter.to_sql(TRANSACTIONS, "transactions")))
                
                logger.debug("Creating events table")
                conn.execute(text(SchemaConverter.to_sql(EVENTS, "events")))
                
                conn.commit()
                logger.info("Successfully created all database tables")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    def _stream_arrow_to_postgresql(self, connection, table: pa.Table, table_name: str, batch_size: int = 10000):
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
                    
                except Exception as e:
                    logger.error(f"Error processing batch {batch_count}: {e}")
                    logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
                    connection.rollback()
                    raise
                    
            logger.info(f"Successfully streamed {batch_count} batches to {table_name} table")
            
        except Exception as e:
            logger.error(f"Error streaming to {table_name}: {e}")
            raise
        finally:
            cursor.close()

    def _prepare_data_for_postgres(self, data: Data) -> Tuple[Optional[pl.DataFrame], Dict[str, pl.DataFrame]]:
        """Prepare and validate data for PostgreSQL insertion"""
        try:
            # Prepare blocks data
            blocks_df = None
            if data.blocks and isinstance(data.blocks, dict):
                all_blocks = []
                for event_name, blocks_df in data.blocks.items():
                    # Convert to arrow and back to get a fresh DataFrame
                    blocks_arrow = blocks_df.to_arrow()
                    fresh_blocks = pl.from_arrow(blocks_arrow)
                    if fresh_blocks.height > 0:
                        logger.debug(f"Processing {fresh_blocks.height} blocks from {event_name}")
                        # Apply schema validation and casting
                        fresh_blocks = fresh_blocks.cast(SchemaConverter.to_polars(BLOCKS))
                        all_blocks.append(fresh_blocks)
                
                if all_blocks:
                    blocks_df = pl.concat(all_blocks).unique(subset=["block_number"]).sort("block_number")
                    logger.info(f"Prepared {blocks_df.height} unique blocks for insertion")
                    logger.debug(f"Block range: {blocks_df['block_number'].min()} to {blocks_df['block_number'].max()}")

            # Prepare events data
            events_dict = {}
            if data.events:
                for event_name, event_df in data.events.items():
                    # Convert to arrow and back to get a fresh DataFrame
                    event_arrow = event_df.to_arrow()
                    fresh_events = pl.from_arrow(event_arrow)
                    if fresh_events.height > 0:
                        logger.debug(f"Processing {fresh_events.height} events from {event_name}")
                        # Apply schema validation and casting
                        fresh_events = fresh_events.cast(SchemaConverter.to_polars(EVENTS))
                        events_dict[event_name] = fresh_events.sort("block_number")
                        logger.debug(f"Event block range: {fresh_events['block_number'].min()} to {fresh_events['block_number'].max()}")

            return blocks_df, events_dict

        except Exception as e:
            logger.error(f"Error preparing data for PostgreSQL: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    async def write(self, data: Data) -> None:
        """Write data into PostgreSQL database"""
        try:
            logger.info("Writing data to PostgreSQL")
            # Create connection using extracted parameters
            connection = await asyncio.to_thread(
                psycopg2.connect,
                **self.conn_params
            )
            
            try:
                # Prepare and validate data
                blocks_df, events_dict = self._prepare_data_for_postgres(data)
                
                # Stream blocks to PostgreSQL
                if blocks_df is not None:
                    logger.info(f"Streaming {blocks_df.height} unique blocks to PostgreSQL")
                    await asyncio.to_thread(
                        self._stream_arrow_to_postgresql,
                        connection, 
                        blocks_df.to_arrow(), 
                        "blocks"
                    )
                
                # Stream events to PostgreSQL
                total_events = 0
                for event_name, event_df in events_dict.items():
                    logger.info(f"Streaming {event_df.height} events for {event_name} to PostgreSQL")
                    await asyncio.to_thread(
                        self._stream_arrow_to_postgresql,
                        connection, 
                        event_df.to_arrow(), 
                        "events"
                    )
                    total_events += event_df.height
                
                if total_events > 0:
                    logger.info(f"Successfully streamed {total_events} total events to PostgreSQL")
                
                await asyncio.to_thread(connection.commit)
                logger.info("Successfully committed all data to PostgreSQL")
                    
            finally:
                await asyncio.to_thread(connection.close)
                logger.debug("Closed PostgreSQL connection")
                
        except Exception as e:
            logger.error(f"Error writing to PostgreSQL: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 