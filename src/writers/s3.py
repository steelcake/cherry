import logging
import asyncio
from datetime import datetime
import polars as pl
from minio import Minio
from src.writers.base import DataWriter
from src.types.data import Data
from typing import Optional
import io
import aioboto3

logger = logging.getLogger(__name__)

class S3Writer(DataWriter):
    """Writer for writing data to S3"""
    def __init__(self, endpoint: str, bucket: str, access_key: Optional[str] = None, 
                 secret_key: Optional[str] = None, region: Optional[str] = None,
                 secure: bool = True):
        super().__init__()
        # Format endpoint URL properly
        if not endpoint.startswith(('http://', 'https://')):
            endpoint = f"{'https://' if secure else 'http://'}{endpoint}"
            
        self.endpoint = endpoint
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.secure = secure
        self.session = aioboto3.Session()
        logger.info(f"Initialized S3Writer with endpoint {endpoint}, bucket {bucket}")
        
        # Initialize S3 client
        self.client = Minio(
            endpoint=endpoint.replace('http://', '').replace('https://', ''),  # Minio needs hostname only
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
            region=self.region
        )
        
        # Ensure bucket exists
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            logger.info(f"Created S3 bucket: {self.bucket}")

    async def write(self, data: Data) -> None:
        """Write data to S3"""
        try:
            # Prepare and validate data using base class method
            blocks_df, events_dict = self.prepare_data(data)
            
            tasks = []
            
            # Queue all events and blocks for parallel upload
            if data.events:
                for event_name, event_df in events_dict.items():
                    tasks.append(self._queue_events(event_name, event_df))
            if blocks_df is not None:
                for event_name in events_dict.keys():
                    tasks.append(self._queue_blocks(event_name, blocks_df))
            
            # Execute all uploads in parallel
            if tasks:
                logger.info(f"Starting parallel upload of {len(tasks)} files to S3")
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Check for any errors
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Error in S3 upload task {i}: {result}")
                        raise result
                
        except Exception as e:
            logger.error(f"Error writing to S3: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise

    async def _queue_events(self, event_name: str, event_df: pl.DataFrame) -> None:
        """Queue event data for S3 upload"""
        try:
            # Generate timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            min_block = event_df['block_number'].min()
            max_block = event_df['block_number'].max()
            key = f"events/{event_name.lower()}/{timestamp}_{min_block}_{max_block}.parquet"
            
            # Write to buffer
            buffer = io.BytesIO()
            event_df.write_parquet(buffer)
            buffer.seek(0)
            
            # Upload to S3
            async with self.session.client(
                's3', endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
                use_ssl=self.secure,
                verify=self.secure
            ) as s3:
                await s3.upload_fileobj(buffer, self.bucket, key)
                
            logger.info(f"Wrote {event_df.height} {event_name} events to s3://{self.bucket}/{key}")
            
        except Exception as e:
            logger.error(f"Error queueing {event_name} events to S3: {e}")
            raise

    async def _queue_blocks(self, event_name: str, blocks_df: pl.DataFrame) -> None:
        """Queue block data for S3 upload"""
        try:
            # Generate timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            min_block = blocks_df['block_number'].min()
            max_block = blocks_df['block_number'].max()
            key = f"blocks/{event_name.lower()}/{timestamp}_{min_block}_{max_block}.parquet"
            
            # Write to buffer
            buffer = io.BytesIO()
            blocks_df.write_parquet(buffer)
            buffer.seek(0)
            
            # Upload to S3
            async with self.session.client(
                's3', endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
                use_ssl=self.secure,
                verify=self.secure
            ) as s3:
                await s3.upload_fileobj(buffer, self.bucket, key)
                
            logger.info(f"Wrote {blocks_df.height} {event_name} blocks to s3://{self.bucket}/{key}")
            
        except Exception as e:
            logger.error(f"Error queueing {event_name} blocks to S3: {e}")
            raise 