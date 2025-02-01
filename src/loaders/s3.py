import logging
import asyncio
from pathlib import Path
from datetime import datetime
import polars as pl
from minio import Minio
from minio.error import S3Error
from src.loaders.base import DataLoader
from src.types.data import Data
from src.schemas.blockchain_schemas import BLOCKS, EVENTS
from src.schemas.base import SchemaConverter
import tempfile
import os
from typing import Optional
import pyarrow as pa
import pyarrow.parquet as pq
import io
import aioboto3

logger = logging.getLogger(__name__)

class S3Loader(DataLoader):
    """Loader for writing data to S3"""
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
        logger.info(f"Initialized S3Loader with endpoint {endpoint}, bucket {bucket}")
        
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
        
        self.events_schema = SchemaConverter.to_polars(EVENTS)
        self.blocks_schema = SchemaConverter.to_polars(BLOCKS)
        
        # Create temp directory
        self.temp_dir = Path(tempfile.gettempdir()) / "blockchain_s3_temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    async def load(self, data: Data) -> None:
        """Load data to S3"""
        try:
            tasks = []
            
            # Queue all events and blocks for parallel upload
            if data.events:
                for event_name, event_df in data.events.items():
                    tasks.append(self._queue_events(event_name, event_df))
            if data.blocks:
                for event_name, blocks_df in data.blocks.items():
                    tasks.append(self._queue_blocks(event_name, blocks_df))
            
            # Execute all uploads in parallel
            if tasks:
                await asyncio.gather(*tasks)
                
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
            
            # Convert to Arrow table with schema
            required_columns = [
                "log_index", "transaction_index", "transaction_hash", 
                "block_number", "address", "data", "topic0", "topic1", 
                "topic2", "topic3"
            ]
            
            # Ensure all required columns exist
            for col in required_columns:
                if col not in event_df.columns:
                    event_df = event_df.with_columns(pl.lit(None).alias(col))
            
            # Select and order columns
            event_df = event_df.select(required_columns + [
                col for col in event_df.columns 
                if col not in required_columns
            ])
            
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
            
            # Convert to Arrow table with schema
            required_columns = ["block_number", "block_timestamp"]
            
            # Ensure all required columns exist
            for col in required_columns:
                if col not in blocks_df.columns:
                    blocks_df = blocks_df.with_columns(pl.lit(None).alias(col))
            
            # Select and order columns
            blocks_df = blocks_df.select(required_columns + [
                col for col in blocks_df.columns 
                if col not in required_columns
            ])
            
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

    async def _write_dataframe_to_s3(self, df: pl.DataFrame, object_name: str, description: str) -> None:
        """Write a DataFrame to S3 as parquet file"""
        temp_file = self.temp_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{object_name.replace('/', '_')}"
        try:
            # Write DataFrame to temporary file
            df.write_parquet(temp_file)
            
            # Upload to S3
            self.client.fput_object(
                bucket_name=self.bucket,
                object_name=object_name,
                file_path=str(temp_file)
            )
            
        except Exception as e:
            logger.error(f"Error uploading {description} to S3: {e}")
            raise
        finally:
            # Clean up temp file
            try:
                if temp_file.exists():
                    temp_file.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {temp_file}: {e}")

    def __del__(self):
        """Cleanup temp directory on object destruction"""
        try:
            if hasattr(self, 'temp_dir') and self.temp_dir.exists():
                for file in self.temp_dir.glob('*'):
                    try:
                        file.unlink()
                    except Exception as e:
                        logger.warning(f"Failed to delete temp file {file}: {e}")
                self.temp_dir.rmdir()
        except Exception as e:
            logger.warning(f"Error cleaning up temp directory: {e}") 