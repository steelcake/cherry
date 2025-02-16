from sqlalchemy import Engine, text
import logging
import pyarrow as pa
import psycopg2
from psycopg2.extensions import connection as pg_connection
from src.schemas.blockchain_schemas import BLOCKS, TRANSACTIONS, EVENTS
from src.schemas.base import SchemaConverter
from src.types.data import Data
from src.writers.base import DataWriter
import polars as pl
from typing import Tuple, Optional, Dict, List, Any
import asyncio
from src.config.parser import Output
from sqlalchemy import create_engine
import pandas as pd
from src.schemas.blockchain_schemas import BlockchainSchema
import time
from io import StringIO
import csv

logger = logging.getLogger(__name__)

class PostgresWriter(DataWriter):
    BLOCKS_TABLE = BlockchainSchema.BLOCKS_TABLE_NAME

    def __init__(self, config: Output):
        self.host = config.host
        self.port = config.port
        self.database = config.database
        self.username = config.username
        self.password = config.password
        self.conn = None
        self.initialized = False
        self._init_task = asyncio.create_task(self._initialize())

    def _connect(self, database: str = None) -> pg_connection:
        """Establish database connection"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=database or self.database,
                connect_timeout=3
            )
            conn.autocommit = True
            return conn
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}")

    async def _initialize(self) -> None:
        """Initialize database and tables"""
        try:
            try:
                self.conn = self._connect()
            except Exception:
                # Create database if it doesn't exist
                sys_conn = self._connect('postgres')
                with sys_conn.cursor() as cur:
                    cur.execute(f'CREATE DATABASE {self.database}')
                sys_conn.close()
                self.conn = self._connect()

            # Create tables
            with self.conn.cursor() as cur:
                cur.execute(BlockchainSchema.get_postgres_blocks_ddl())
                cur.execute(BlockchainSchema.get_postgres_events_ddl('approval_events'))
                cur.execute(BlockchainSchema.get_postgres_events_ddl('transfer_events'))
            
            self.initialized = True
            logger.info(f"Initialized PostgreSQL connection to {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL: {str(e)}")
            raise

    def _prepare_data(self, batch: pa.RecordBatch) -> pd.DataFrame:
        """Convert Arrow batch to DataFrame with proper types"""
        df = batch.to_pandas()
        
        # Convert timestamps
        if 'block_timestamp' in df.columns:
            df['block_timestamp'] = pd.to_datetime(
                df['block_timestamp'].apply(lambda x: int(x[2:], 16) if isinstance(x, str) and x.startswith('0x') else x),
                unit='s'
            )
        
        # Convert numeric columns
        for col in ['log_index', 'transaction_index', 'block_number']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    async def _insert_data(self, table_name: str, df: pd.DataFrame) -> None:
        """Insert data using COPY command"""
        if df.empty:
            return

        output = StringIO()
        writer = csv.writer(output, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(df.values.tolist())
        output.seek(0)

        with self.conn.cursor() as cur:
            if table_name == 'blocks':
                # Use temp table for blocks to handle upserts
                temp_table = f"temp_blocks_{int(time.time())}"
                cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE blocks INCLUDING ALL)")
                cur.copy_expert(f"COPY {temp_table} ({','.join(df.columns)}) FROM STDIN WITH CSV DELIMITER E'\\t' QUOTE '\"'", output)
                
                cur.execute(f"""
                    INSERT INTO blocks 
                    SELECT * FROM {temp_table}
                    ON CONFLICT (block_number) 
                    DO UPDATE SET 
                        block_timestamp = EXCLUDED.block_timestamp,
                        block_hash = EXCLUDED.block_hash,
                        block_parent_hash = EXCLUDED.block_parent_hash,
                        block_transaction_count = EXCLUDED.block_transaction_count,
                        block_size = EXCLUDED.block_size
                """)
                cur.execute(f"DROP TABLE {temp_table}")
            else:
                cur.copy_expert(f"COPY {table_name} ({','.join(df.columns)}) FROM STDIN WITH CSV DELIMITER E'\\t' QUOTE '\"'", output)

        output.close()
        logger.info(f"Inserted {len(df)} rows into {table_name}")

    async def push_data(self, data: dict[str, pa.RecordBatch]) -> None:
        """Push data to PostgreSQL"""
        if not self.initialized:
            await self._init_task

        # Process events
        for table_name in [name for name in data if name.endswith('_events')]:
            df = self._prepare_data(data[table_name])
            await self._insert_data(table_name, df)

        # Process blocks
        blocks_tables = [name for name in data if name.startswith('blocks_')]
        if blocks_tables:
            blocks_dfs = [self._prepare_data(data[table]) for table in blocks_tables]
            combined_blocks = pd.concat(blocks_dfs).drop_duplicates(subset=['block_number'])
            await self._insert_data('blocks', combined_blocks) 