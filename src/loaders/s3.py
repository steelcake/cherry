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

logger = logging.getLogger(__name__)

class S3Loader(DataLoader):
    """Handles writing data to S3-compatible storage"""
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str, secure: bool = True):
        super().__init__()
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.bucket = bucket
        self._ensure_bucket()
        # Create temp directory
        self.temp_dir = Path(tempfile.gettempdir()) / "blockchain_s3_temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized S3Loader with endpoint {endpoint}, bucket {bucket}")

    def _ensure_bucket(self):
        """Ensure bucket exists, create if it doesn't"""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"Created bucket {self.bucket}")
            else:
                logger.debug(f"Bucket {self.bucket} already exists")
        except S3Error as e:
            logger.error(f"Error ensuring bucket exists: {e}")
            raise

    async def load(self, data: Data) -> None:
        """Load data to S3 storage"""
        try:
            if not data or (not data.events and not data.blocks):
                return

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            write_tasks = []

            # Get block range for filenames
            min_block = float('inf')
            max_block = 0
            if data.events:
                for event_df in data.events.values():
                    if event_df.height > 0:
                        min_block = min(min_block, event_df['block_number'].min())
                        max_block = max(max_block, event_df['block_number'].max())

            # Process events data
            if data.events:
                for event_name, event_df in data.events.items():
                    if event_df.height > 0:
                        write_tasks.append(
                            asyncio.create_task(
                                self._write_dataframe_to_s3(
                                    event_df,
                                    f"events/{event_name}/events_{timestamp}_block_{min_block}_to_{max_block}.parquet",
                                    f"events for {event_name}"
                                )
                            )
                        )

            # Process blocks data
            if data.blocks:
                for event_name, block_df in data.blocks.items():
                    if block_df.height > 0:
                        unique_blocks = block_df.unique(subset=["block_number"]).sort("block_number")
                        write_tasks.append(
                            asyncio.create_task(
                                self._write_dataframe_to_s3(
                                    unique_blocks,
                                    f"blocks/{event_name}/blocks_{timestamp}_block_{min_block}_to_{max_block}.parquet",
                                    f"blocks for {event_name}"
                                )
                            )
                        )

            if write_tasks:
                await asyncio.gather(*write_tasks)
                logger.info("Successfully uploaded all data to S3")

        except Exception as e:
            logger.error(f"Error writing to S3: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
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
            
            logger.info(f"Uploaded {df.height} {description} to s3://{self.bucket}/{object_name}")
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