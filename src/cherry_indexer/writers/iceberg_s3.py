import asyncio
import logging
from typing import Dict
from ..writers.base import DataWriter
from ..config.parser import WriterConfig
import pyarrow as pa
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType, StringType, LongType, DoubleType, TimestampType
)
from pyiceberg.schema import NestedField
from pyiceberg.catalog.sql import SqlCatalog
import os
import boto3
import time

logger = logging.getLogger(__name__)

def _get_iceberg_schema(record_batch: pa.RecordBatch) -> Schema:
    """Convert PyArrow schema to Iceberg schema"""
    schema_fields = []
    field_id = 1
    
    for field in record_batch.schema:
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
    
    return Schema(*schema_fields)

class IcebergWriter(DataWriter):
    def __init__(self, config: WriterConfig):
        logger.info("Initializing Iceberg S3 writer...")
        self._init_s3_config(config)
        self._init_minio()
        logger.info(f"Initialized IcebergWriter with endpoint {self.endpoint_url}")

    def _init_s3_config(self, config: WriterConfig) -> None:
        """Initialize S3 configuration"""
        self.endpoint_url = config.endpoint
        self.s3_path = f"s3://{config.s3_path}" if not config.s3_path.startswith('s3://') else config.s3_path
        self.database = config.database or 'blockchain'
        self.bucket_name = self.s3_path.split("/")[2]
        logger.info(f"Using S3 path: {self.s3_path}")

    def _init_minio(self) -> None:
        """Initialize MinIO connection"""
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            verify=False,
            config=boto3.session.Config(
                connect_timeout=60,
                read_timeout=60,
                retries={'max_attempts': 3},
                signature_version='s3v4',
                s3={'addressing_style': 'path'}
            )
        )
        
        # Create bucket if it doesn't exist
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket exists: {self.bucket_name}")
        except:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
                time.sleep(2)  # Wait for bucket creation
            except Exception as e:
                logger.error(f"Failed to create bucket {self.bucket_name}: {e}")
                raise
        
        # Initialize Iceberg catalog with filesystem configuration
        try:
            warehouse_path = f"{self.s3_path}/iceberg-warehouse"
            catalog_dir = os.path.join(os.path.expanduser("~"), ".iceberg")
            os.makedirs(catalog_dir, exist_ok=True)
            catalog_path = os.path.join(catalog_dir, "catalog.db")

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
                    region_name="us-east-1",
                    allow_http=True,
                    client_config=dict(
                        max_pool_connections=30,
                        connect_timeout=60,
                        read_timeout=60,
                        retries=dict(
                            max_attempts=5,
                            mode="standard"
                        )
                    )
                )
            )

            # Create namespace if it doesn't exist
            try:
                self.catalog.create_namespace(
                    (self.database,),
                    properties={
                        "location": f"{self.s3_path}/{self.database}",
                        "fs.s3a.path.style.access": "true",
                        "fs.s3a.connection.ssl.enabled": "false",
                        "fs.s3a.endpoint": self.endpoint_url.replace("http://", "")
                    }
                )
                logger.info(f"Created namespace: {self.database}")
            except Exception as e:
                logger.warning(f"Namespace creation: {e}")

        except Exception as e:
            logger.error(f"Failed to initialize Iceberg catalog: {e}")
            raise
    async def write_table(self, table_name: str, record_batch: pa.RecordBatch) -> None:
        """Write data to S3 using Iceberg"""
        logger.info(f"Writing table: {table_name}")
        
        table = pa.Table.from_batches([record_batch])
        df = table.to_pandas()
        
        schema = _get_iceberg_schema(record_batch)
        table_identifier = f"{self.database}.{table_name}"
        
        try:
            # Create or load Iceberg table
            if not self.catalog.table_exists(table_identifier):
                self.catalog.create_table(
                    identifier=table_identifier,
                    schema=schema,
                    location=f"{self.s3_path}/{table_name}",
                    properties={
                        "write.format.default": "parquet",
                        "write.metadata.compression-codec": "none",
                        "write.parquet.compression-codec": "snappy"
                    }
                )
            
            iceberg_table = self.catalog.load_table(table_identifier)
            
            # Write data using Iceberg
            with iceberg_table.transaction() as tx:
                tx.append(df)
                
            logger.info(f"Successfully wrote {len(df)} rows to Iceberg table {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to write to Iceberg table {table_name}: {e}")
            raise
        # Convert to parquet
        parquet_buffer = pa.BufferOutputStream()
        pa.parquet.write_table(table, parquet_buffer)
        
        # Write to S3
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"{self.database}/{table_name}/data.parquet",
                Body=parquet_buffer.getvalue().to_pybytes()
            )
            logger.info(f"Successfully wrote {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Failed to write {table_name}: {e}")
            raise

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to S3"""
        try:
            for table_name, record_batch in data.items():
                await self.write_table(table_name, record_batch)
        except Exception as e:
            logger.error(f"Error writing to S3: {e}")
            raise