import asyncio
import logging
from typing import Dict
from ..writers.base import DataWriter
from ..config.parser import WriterConfig
import pyarrow as pa
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType, StringType, LongType, DoubleType, TimestampType
)
from pyiceberg.schema import NestedField
from pyiceberg.catalog import Catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import TableProperties
from pyiceberg.exceptions import NoSuchNamespaceError
import os
from pyarrow.fs import S3FileSystem
import time
import boto3

logger = logging.getLogger(__name__)

def _get_iceberg_schema(record_batch: pa.RecordBatch) -> Schema:
    """Convert PyArrow schema to Iceberg schema"""
    schema_fields = []
    field_id = 1  # Iceberg requires unique field IDs
    
    for field in record_batch.schema:
        try:
            field_type = str(field.type)
            if field_type == 'bool':
                iceberg_type = BooleanType()
            elif field_type in ['int64', 'uint64']:
                iceberg_type = LongType()
            elif field_type == 'double':
                iceberg_type = DoubleType()
            elif field_type == 'timestamp[us]':
                iceberg_type = TimestampType()
            else:
                iceberg_type = StringType()
            
            schema_fields.append(
                NestedField(
                    field_id=field_id,
                    name=field.name,
                    field_type=iceberg_type,
                    required=False
                )
            )
            field_id += 1
            
        except Exception as e:
            logger.warning(f"Could not convert field {field.name} of type {field.type}: {e}")
            schema_fields.append(
                NestedField(
                    field_id=field_id,
                    name=field.name,
                    field_type=StringType(),
                    required=False
                )
            )
            field_id += 1
    
    return Schema(*schema_fields)

class IcebergWriter(DataWriter):
    def __init__(self, config: WriterConfig):
        logger.info("Initializing Iceberg S3 writer...")
        self._init_s3_config(config)
        self._init_catalog()
        logger.info(f"Initialized IcebergWriter with endpoint {self.endpoint_url}")

    def _init_s3_config(self, config: WriterConfig) -> None:
        """Initialize S3 configuration"""
        self.endpoint_url = config.endpoint
        
        if not config.s3_path.startswith('s3://'):
            self.s3_path = f"s3://{config.s3_path}"
        else:
            self.s3_path = config.s3_path
            
        logger.info(f"Using S3 path: {self.s3_path}")
        
        self.partition_cols = config.partition_cols or {}
        self.default_partition_cols = config.default_partition_cols
        self.anchor_table = config.anchor_table or 'blocks'
        self.database = config.database or 'blockchain'

    def _init_catalog(self) -> None:
        """Initialize Iceberg catalog"""
        warehouse_path = f"{self.s3_path}/iceberg-warehouse"
        
        # Create a local SQLite catalog file
        catalog_dir = os.path.join(os.path.expanduser("~"), ".iceberg")
        os.makedirs(catalog_dir, exist_ok=True)
        catalog_path = os.path.join(catalog_dir, "catalog.db")

        # Configure S3 filesystem with correct parameters
        s3_fs = S3FileSystem(
            access_key="minioadmin",
            secret_key="minioadmin",
            endpoint_override=self.endpoint_url,
            scheme="http",
            allow_bucket_creation=True,
            background_writes=False,  # Disable background writes for better stability
            request_timeout=60,  # Reduced timeout
            connect_timeout=30,
        )
        
        # Ensure the base paths exist in MinIO
        try:
            s3_fs.create_dir("blockchain-data/iceberg-warehouse")
            s3_fs.create_dir("blockchain-data/iceberg-s3")
            logger.info("Created base directories in MinIO")
        except Exception as e:
            logger.warning(f"Error creating directories (may already exist): {e}")
        
        # Configure catalog with SQLite backend
        self.catalog = SqlCatalog(
            name="cherry",
            uri=f"sqlite:///{catalog_path}",
            warehouse=warehouse_path,
            s3=dict(
                endpoint_url=self.endpoint_url,
                access_key_id="minioadmin",
                secret_access_key="minioadmin",
                path_style_request=True,
                use_ssl=False,
                verify=False,
                connect_timeout=30,
                read_timeout=60,
                retries=dict(
                    max_attempts=5,
                    mode="standard"
                )
            ),
            io=s3_fs
        )
        
        # Create namespace if it doesn't exist
        try:
            self.catalog.create_namespace(
                (self.database,),
                properties={
                    "location": f"{self.s3_path}/{self.database}",
                    "write.metadata.previous-versions-max": "1"
                }
            )
            logger.info(f"Created namespace: {self.database}")
        except Exception as e:
            if not isinstance(e, NoSuchNamespaceError):
                logger.warning(f"Error creating namespace: {e}")
        
        # Initialize FileIO for S3
        self.file_io = PyArrowFileIO(
            properties={
                "s3.endpoint": self.endpoint_url,
                "s3.access-key-id": "minioadmin",
                "s3.secret-access-key": "minioadmin",
                "s3.path-style-access": "true",
                "s3.ssl-enabled": "false",
                "s3.verify-ssl": "false",
                "s3.connection-timeout-ms": "300000",
                "s3.retry.limit": "5"
            }
        )

    async def _write_to_iceberg(self, df: pd.DataFrame, table_name: str, schema: Schema) -> None:
        """Write DataFrame to S3 using Iceberg format"""
        def write():
            max_retries = 3
            retry_delay = 1  # seconds
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Writing {len(df)} rows to {table_name} (attempt {attempt + 1}/{max_retries})")
                    table_identifier = f"{self.database}.{table_name}"
                    
                    # Create table if it doesn't exist
                    if not self.catalog.table_exists(table_identifier):
                        self.catalog.create_table(
                            identifier=table_identifier,
                            schema=schema,
                            location=f"{self.s3_path}/{table_name}",
                            properties={
                                "write.format.default": "parquet",
                                "write.metadata.compression-codec": "none",
                                "write.parquet.compression-codec": "snappy",
                                "write.metadata.previous-versions-max": "1",
                                "write.object-storage.enabled": "true",
                                "write.delete.mode": "copy-on-write"
                            }
                        )
                    
                    table = self.catalog.load_table(table_identifier)
                    
                    # Handle non-UTF-8 columns
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else x)
                    
                    # Write to table using pyiceberg's write API
                    with table.transaction() as tx:
                        tx.append(df)
                    
                    logger.info(f"Successfully wrote {len(df)} rows to {table_name}")
                    return  # Success, exit retry loop
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Attempt {attempt + 1} failed for {table_name}: {e}")
                        time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    else:
                        logger.error(f"Failed to write to Iceberg table after {max_retries} attempts: {e}")
                        raise

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, write)

    async def write_table(self, table_name: str, record_batch: pa.RecordBatch) -> None:
        """Write data to S3 as Iceberg table"""
        logger.info(f"Starting Iceberg write for table {table_name}")
        
        table = pa.Table.from_batches([record_batch])
        pandas_df = table.to_pandas()
        
        schema = _get_iceberg_schema(record_batch)
        await self._write_to_iceberg(pandas_df, table_name, schema)

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to S3"""
        try:
            logger.info(f"Starting push_data for {len(data)} tables: {', '.join(data.keys())}")

            if self.anchor_table is None:
                await self._write_tables(data)
            else:
                # Split data into non-anchor and anchor tables
                non_anchor_data = {k: v for k, v in data.items() if k != self.anchor_table}
                anchor_data = {k: v for k, v in data.items() if k == self.anchor_table}
                
                # Write non-anchor tables first
                if non_anchor_data:
                    await self._write_tables(non_anchor_data)
                
                # Write anchor table last
                if anchor_data:
                    await self._write_tables(anchor_data)
            
        except Exception as e:
            logger.error(f"Error writing to Iceberg tables: {e}")
            raise

    async def _write_tables(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write tables to Iceberg format"""
        event_tasks = {
            name: asyncio.create_task(self.write_table(name, df), name=f"write_{name}")
            for name, df in data.items()
        }
        
        for name, task in event_tasks.items():
            try:
                await task
            except Exception as e:
                logger.error(f"Failed to write {name}: {str(e)}")
                raise Exception(f"Error writing Iceberg table {name}: {e}")