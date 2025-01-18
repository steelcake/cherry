from sqlalchemy import Engine, create_engine, text
import logging
import pyarrow as pa
import psycopg2
from src.schemas.blockchain_schemas import BLOCKS, TRANSACTIONS, EVENTS
from src.ingesters.base import Data
from src.loaders.base import DataLoader
import polars as pl

logger = logging.getLogger(__name__)

class PostgresLoader(DataLoader):
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        logger.info("Creating database tables if they don't exist")
        try:
            with self.engine.connect() as conn:
                logger.debug("Creating blocks table")
                conn.execute(text(BLOCKS.to_sql()))
                
                logger.debug("Creating transactions table")
                conn.execute(text(TRANSACTIONS.to_sql()))
                
                logger.debug("Creating events table")
                conn.execute(text(EVENTS.to_sql()))
                
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

    async def write_data(self, data: Data, **kwargs) -> None:
        """Write data to PostgreSQL"""
        try:
            logger.info("Writing data to PostgreSQL")
            connection = self.engine.raw_connection()
            
            try:
                # Stream blocks to PostgreSQL if present
                if data.blocks and isinstance(data.blocks, dict):
                    logger.info(f"Processing blocks from {len(data.events)} events")
                    # Combine all block DataFrames and deduplicate
                    all_blocks = []
                    for event_name, blocks_df in data.blocks.items():
                        if blocks_df.height > 0:
                            all_blocks.append(blocks_df)
                    
                    if all_blocks:
                        combined_blocks = pl.concat(all_blocks).unique(subset=["block_number"])
                        logger.info(f"Streaming {combined_blocks.height} unique blocks to PostgreSQL")
                        self._stream_arrow_to_postgresql(
                            connection, 
                            combined_blocks.to_arrow(), 
                            "blocks"
                        )
                
                # Stream events to PostgreSQL
                if data.events:
                    for event_name, event_df in data.events.items():
                        if event_df.height > 0:
                            logger.info(f"Streaming {event_df.height} events for {event_name} to PostgreSQL")
                            self._stream_arrow_to_postgresql(
                                connection, 
                                event_df.to_arrow(), 
                                "events"
                            )
                
                logger.info("Successfully wrote data to PostgreSQL")
                        
            finally:
                connection.close()
                    
        except Exception as e:
            logger.error(f"Error writing to PostgreSQL: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 