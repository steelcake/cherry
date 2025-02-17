import asyncio, boto3, logging, os
from typing import Optional, Dict, List
from writers.base import DataWriter
from config.parser import Output
import pyarrow as pa, pandas as pd
from concurrent.futures import ThreadPoolExecutor
from writers.writer import get_output_path
from datetime import datetime

logger = logging.getLogger(__name__)

def _get_athena_schema(record_batch: pa.RecordBatch) -> Dict[str, str]:
        """Convert PyArrow schema to Athena compatible types"""
        return {
            field.name: wr._data_types.pyarrow2athena(field.type)
            for field in record_batch.schema
        }

class AWSWranglerWriter(DataWriter):
    def __init__(self, config: Output):
        logger.info("Initializing AWS Wrangler S3 writer...")
        self._init_s3_config(config)
        
        if config.use_boto3:
            self._init_session(config)
        
        logger.info(f"Initialized AWSWranglerWriter with endpoint {self.endpoint_url}")

    def _init_session(self, config: Output) -> None:
        """Initialize AWS session"""
        
        self.session = boto3.Session(
            region_name=config.region or 'us-east-1'
        )
        
        # Configure AWS Wrangler
        wr.config.s3_endpoint_url = self.endpoint_url

    def _init_s3_config(self, config: Output) -> None:
        """Initialize S3 configuration"""
        # Format endpoint URL
        self.endpoint_url = config.endpoint
        self.s3_path = config.s3_path
            
        logger.info(f"Using S3 path: {self.s3_path}")
        
        self.partition_cols = config.partition_cols or {}
        self.default_partition_cols = config.default_partition_cols

    async def write_parquet(self, table_name: str, record_batch: pa.RecordBatch) -> None:
        """Write data to S3 as Parquet"""
        logger.info(f"Starting Parquet write for table {table_name}")
        
        table = pa.Table.from_batches([record_batch])
        pandas_df = table.to_pandas()

        schema = _get_athena_schema(record_batch)
        await self._write_to_s3(pandas_df, table_name, schema)

    async def _write_to_s3(self, df: pd.DataFrame, table_name: str, schema: Dict[str, str]) -> None:
        """Write DataFrame to S3 using thread pool"""
        def write():
            logger.info(f"Writing {len(df)} rows to {table_name}")
            wr.s3.to_parquet(
                df=df,
                boto3_session=self.session if self.session else None,
                path=self.s3_path,
                dataset=True,
                use_threads=True,
                mode="append",
                database=self.database,
                table=table_name,
                dtype=schema,
                partition_cols=self.partition_cols.get(table_name, self.default_partition_cols),
                schema_evolution=False,
            )
            logger.info(f"Successfully wrote {len(df)} rows to {self.s3_path}")

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, write)

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to S3"""
        try:
            logger.info(f"Starting push_data for {len(data)} tables: {', '.join(data.keys())}")

            if self.anchor_table is None:
                await self._write_tables(data)
            else:
                await self._write_tables(filter(lambda x, _: x != self.anchor_table, data))
                await self._write_tables(filter(lambda x, _: x == self.anchor_table, data))
            
        except Exception as e:
            logger.error(f"Error writing to S3: {e}")
            raise

    async def _write_tables(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write event data to S3"""
        event_tasks = {
            name: asyncio.create_task(self.write_parquet(name, df), name=f"write_{name}")
            for name, df in data.items()
        }
        
        for name, task in event_tasks.items():
            try:
                await task
            except Exception as e:
                logger.error(f"Failed to write {name}: {str(e)}")
                raise Exception(f"Error writing parquet table into {name}: {e}")

    def _get_s3_path(self, table: str, min_block: int, max_block: int) -> str:
        """Generate S3 path for the table"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_path = f"s3://{self.bucket}/{self.s3_path}"
        
        if table.endswith('_events'):
            event_type = table.replace('_events', '')
            return f"{base_path}/events/{event_type}/{timestamp}_{min_block}_{max_block}.parquet"
        else:
            return f"{base_path}/blocks/{timestamp}_{min_block}_{max_block}.parquet"
