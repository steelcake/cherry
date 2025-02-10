import asyncio, boto3, logging, os
from typing import Optional, Dict, List
from src.writers.base import DataWriter
from src.config.parser import Output
import pyarrow as pa, pandas as pd
import awswrangler as wr
from concurrent.futures import ThreadPoolExecutor
from src.utils.writer import get_output_path
from src.schemas.athena import get_athena_schema
from datetime import datetime

logger = logging.getLogger(__name__)

class AWSWranglerWriter(DataWriter):
    def __init__(self, config: Output):
        logger.info("Initializing AWS Wrangler S3 writer...")
        self._init_s3_config(config)
        self._init_session(config)
        logger.info(f"Initialized AWSWranglerWriter with endpoint {self.endpoint_url}")

    def _init_session(self, config: Output) -> None:
        """Initialize AWS session"""
        # Set AWS credentials in environment
        os.environ['AWS_ACCESS_KEY_ID'] = config.access_key or 'minioadmin'
        os.environ['AWS_SECRET_ACCESS_KEY'] = config.secret_key or 'minioadmin'
        
        self.session = boto3.Session(
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            region_name=config.region or 'us-east-1'
        )
        
        # Configure AWS Wrangler
        wr.config.s3_endpoint_url = self.endpoint_url
        wr.config.s3_verify = config.secure
        wr.config.s3_allow_unsafe_rename = True

    def _init_s3_config(self, config: Output) -> None:
        """Initialize S3 configuration"""
        # Format endpoint URL
        self.endpoint_url = (
            f"{'https://' if config.secure else 'http://'}{config.endpoint}"
            if not config.endpoint.startswith(('http://', 'https://'))
            else config.endpoint
        )
        
        # Handle bucket and path
        if config.s3_path:
            # Extract bucket and path from s3_path if provided
            parts = config.s3_path.split('/', 1)
            self.bucket = config.bucket or parts[0]
            self.s3_path = parts[1] if len(parts) > 1 else ''
        else:
            self.bucket = config.bucket
            self.s3_path = "aws-wrangler-s3"

        if not self.bucket:
            raise ValueError("S3 bucket name is required (either in bucket or s3_path)")
            
        logger.info(f"Using S3 path: {self.bucket}/{self.s3_path}")
        
        self.partition_cols = config.partition_cols or {}
        self.default_partition_cols = config.default_partition_cols

    def _convert_to_table(self, df: pa.RecordBatch | pa.Table) -> pa.Table:
        """Convert input to PyArrow Table"""
        if isinstance(df, pa.RecordBatch):
            return pa.Table.from_batches([df])
        elif isinstance(df, pa.Table):
            return df
        raise TypeError(f"Expected RecordBatch or Table, got {type(df)}")

    async def write_parquet(self, table_name: str, df: pa.RecordBatch) -> None:
        """Write data to S3 as Parquet"""
        logger.info(f"Starting Parquet write for table {table_name}")
        
        table = self._convert_to_table(df)
        pandas_df = table.to_pandas()
        start_block = pandas_df['block_number'].min()
        end_block = pandas_df['block_number'].max()
        full_path = self._get_s3_path(table_name, start_block, end_block)
        
        await self._write_to_s3(pandas_df, full_path, table)

    async def _write_to_s3(self, df: pd.DataFrame, path: str, table: pa.Table) -> None:
        """Write DataFrame to S3 using thread pool"""
        def write():
            logger.info(f"Writing {len(df)} rows to {path}")
            wr.s3.to_parquet(
                df=df,
                path=path,
                index=False,
                compression=None,
                boto3_session=self.session,
                dataset=False,
                dtype={k: v for k, v in get_athena_schema(table).items() 
                      if not k.startswith('__index_')}
            )
            logger.info(f"Successfully wrote {len(df)} rows to {path}")

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, write)

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to S3"""
        try:
            logger.info(f"Starting push_data for {len(data)} tables: {', '.join(data.keys())}")
            
            # Log event statistics
            self.log_event_stats(data)
            
            await self._write_events(data)
            await self._write_blocks(data)
            
        except Exception as e:
            logger.error(f"Error writing to S3: {e}")
            raise

    async def _write_events(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write event data to S3"""
        event_tasks = {
            name: asyncio.create_task(self.write_parquet(name, df), name=f"write_{name}")
            for name, df in data.items() 
            if name.endswith('_events')
        }
        
        for name, task in event_tasks.items():
            try:
                await task
            except Exception as e:
                logger.error(f"Failed to write {name}: {str(e)}")
                raise Exception(f"Error writing parquet table into {name}: {e}")

    async def _write_blocks(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write combined blocks data to S3"""
        blocks_tables = [name for name in data if name.startswith('blocks_')]
        if blocks_tables:
            try:
                blocks_data = self.combine_blocks(data)
                await self.write_parquet('blocks', blocks_data)
            except Exception as e:
                logger.error(f"Error writing blocks anchor table: {e}")
                raise

    def _get_s3_path(self, table: str, min_block: int, max_block: int) -> str:
        """Generate S3 path for the table"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_path = f"s3://{self.bucket}/{self.s3_path}"
        
        if table.endswith('_events'):
            event_type = table.replace('_events', '')
            return f"{base_path}/events/{event_type}/{timestamp}_{min_block}_{max_block}.parquet"
        else:
            return f"{base_path}/blocks/{timestamp}_{min_block}_{max_block}.parquet"
