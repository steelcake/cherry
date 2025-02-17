import logging
import asyncio
from typing import Dict, List
import clickhouse_connect
import pyarrow as pa
import pandas as pd
from ..writers.base import DataWriter
from ..config.parser import Output

logger = logging.getLogger(__name__)

class ClickHouseWriter(DataWriter):
    """Writer for ClickHouse database"""
    
    def __init__(self, config: Output):
        self.client = clickhouse_connect.get_client(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            database=config.database,
            secure=config.secure
        )
        logger.info(f"Initialized ClickHouse connection to {config.host}:{config.port}")
        self._create_tables()

    def _create_tables(self) -> None:
        """Create required tables"""
        try:
            self.client.command(BlockchainSchema.get_clickhouse_blocks_ddl())
            logger.info("Created ClickHouse tables")
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            raise

    def _prepare_data(self, df: pd.DataFrame) -> List[List[str]]:
        """Prepare DataFrame for ClickHouse insert"""
        df = df.copy()
        
        # Convert hex values to integers
        for col in df.columns:
            if col in BlockchainSchema.HEX_COLUMNS and df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: 
                    int(x[2:], 16) if pd.notnull(x) and str(x).startswith('0x') else 0
                )
        
        # Convert timestamps
        for col in BlockchainSchema.TIMESTAMP_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col].astype(int), unit='s')
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert all to strings
        for col in df.columns:
            df[col] = df[col].fillna('').astype(str)
            
        return df.values.tolist()

    async def _insert_data(self, table_name: str, data: List[List[str]], columns: List[str]) -> None:
        """Insert data into ClickHouse table"""
        if not data:
            return
            
        logger.info(f"Writing {len(data)} rows to {table_name}")
        await asyncio.to_thread(
            self.client.insert,
            table_name,
            data,
            column_names=columns
        )
        logger.info(f"Wrote {len(data)} rows to {table_name}")

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write data to ClickHouse"""
        try:
            # Process events
            for table_name in [t for t in data if t.endswith('_events')]:
                df = data[table_name].to_pandas()
                prepared_data = self._prepare_data(df)
                await self._insert_data(table_name, prepared_data, list(df.columns))

            # Process blocks
            blocks_data = self.combine_blocks(data)
            if blocks_data:
                df = blocks_data.to_pandas()
                prepared_data = self._prepare_data(df)
                await self._insert_data('blocks', prepared_data, list(df.columns))
                
        except Exception as e:
            logger.error(f"ClickHouse write failed: {str(e)}")
            raise
        