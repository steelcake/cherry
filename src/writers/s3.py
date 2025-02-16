import logging
import asyncio
from datetime import datetime
from minio import Minio
from src.writers.base import DataWriter
from typing import Dict
import io
import pyarrow as pa
import pyarrow.parquet as pq
from src.config.parser import Output

logger = logging.getLogger(__name__)

class S3Writer(DataWriter):
    def __init__(self, config: Output):
        logger.info("Initializing S3 writer...")
        self._init_config(config)
        logger.info(f"Initialized S3Writer with endpoint {self.endpoint_url}")

    def _init_config(self, config: Output) -> None:
        """Initialize S3 configuration"""
        # Format endpoint URL
        if not config.endpoint.startswith(('http://', 'https://')):
            self.endpoint_url = f"{'https://' if config.secure else 'http://'}{config.endpoint}"
            minio_endpoint = config.endpoint
        else:
            self.endpoint_url = config.endpoint
            minio_endpoint = config.endpoint.replace('http://', '').replace('https://', '')
            
        # Set bucket and path
        self.bucket = config.bucket
        self.s3_path = "minio-s3"  # Fixed subfolder for consistency
        
        # Initialize Minio client
        self.client = Minio(
            endpoint=minio_endpoint,
            access_key=config.access_key or 'minioadmin',
            secret_key=config.secret_key or 'minioadmin',
            secure=config.secure if config.secure is not None else True,
            region=config.region
        )
        
        # Ensure bucket exists
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            logger.info(f"Created bucket: {self.bucket}")

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to S3"""
        try:
            # Process events
            for table_name in [t for t in data if t.endswith('_events')]:
                df = data[table_name].to_pandas()
                min_block = df['block_number'].min()
                max_block = df['block_number'].max()
                await self._write_parquet(table_name, pa.Table.from_pandas(df), min_block, max_block)

            # Process blocks
            blocks_data = self.combine_blocks(data)
            if blocks_data:
                df = blocks_data.to_pandas()
                min_block = df['block_number'].min()
                max_block = df['block_number'].max()
                await self._write_parquet('blocks', blocks_data, min_block, max_block)
                
        except Exception as e:
            logger.error(f"S3 write failed: {str(e)}")
            raise

    async def _write_parquet(self, table_name: str, data: pa.Table, min_block: int, max_block: int) -> None:
        """Write data to S3 as Parquet"""
        try:
            # Generate path
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            subfolder = 'events/' + table_name.replace('_events', '') if table_name.endswith('_events') else 'blocks'
            key = f"{self.s3_path}/{subfolder}/{timestamp}_{min_block}_{max_block}.parquet"
            
            # Write to buffer
            buffer = io.BytesIO()
            pq.write_table(data, buffer)
            buffer.seek(0)
            size = buffer.getbuffer().nbytes
            
            # Upload to S3
            self.client.put_object(
                bucket_name=self.bucket,
                object_name=key,
                data=buffer,
                length=size
            )
            
            logger.info(f"Wrote {len(data)} rows to s3://{self.bucket}/{key}")
            
        except Exception as e:
            logger.error(f"Failed to write {table_name}: {str(e)}")
            raise
        finally:
            buffer.close() 