import asyncio
import logging
import boto3
import pyarrow as pa
import pandas as pd
from typing import Dict, Optional, Type

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import Table
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType,
    FloatType, DoubleType, BooleanType, DateType, TimestampType,
    DecimalType, BinaryType, ListType, StructType
)

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from ..writers.base import DataWriter
from ..config.parser import WriterConfig

logger = logging.getLogger(__name__)

def _convert_pyarrow_to_iceberg_schema(record_batch: pa.RecordBatch) -> Schema:
    """Convert PyArrow schema to Iceberg schema"""
    field_id_counter = [1]  # Use list to allow mutation in nested functions
    
    def get_iceberg_type(pa_type: pa.DataType) -> Type:
        # Map PyArrow types to Iceberg types
        type_map = {
            pa.int8(): IntegerType(),
            pa.int16(): IntegerType(),
            pa.int32(): IntegerType(),
            pa.int64(): LongType(),
            pa.float32(): FloatType(),
            pa.float64(): DoubleType(),
            pa.string(): StringType(),
            pa.binary(): BinaryType(),
            pa.bool_(): BooleanType(),
            pa.date32(): DateType(),
            pa.timestamp('us'): TimestampType(),
            pa.decimal128(38, 18): DecimalType(38, 18),
        }
        
        # Handle nested types if needed
        if isinstance(pa_type, pa.ListType):
            element_field_id = field_id_counter[0]
            field_id_counter[0] += 1
            element_type = get_iceberg_type(pa_type.value_type)
            return ListType(
                element_id=element_field_id,
                element=element_type,
                element_required=not pa_type.value_field.nullable
            )
        elif isinstance(pa_type, pa.StructType):
            fields = []
            for field in pa_type:
                field_id = field_id_counter[0]
                field_id_counter[0] += 1
                field_type = get_iceberg_type(field.type)
                fields.append(
                    NestedField(
                        field_id=field_id,
                        name=field.name,
                        field_type=field_type,
                        required=not field.nullable
                    )
                )
            return StructType(fields=fields)
            
        # Get the matching Iceberg type or default to string
        return type_map.get(pa_type, StringType())

    # Convert each field to Iceberg schema field
    schema_fields = []
    for field in record_batch.schema:
        field_id = field_id_counter[0]
        field_id_counter[0] += 1
        iceberg_type = get_iceberg_type(field.type)
        schema_fields.append(
            NestedField(
                field_id=field_id,
                name=field.name,
                field_type=iceberg_type,
                required=not field.nullable
            )
        )

    return Schema(fields=schema_fields)  # Pass fields as named argument

class IcebergWriter(DataWriter):
    def __init__(self, config: WriterConfig):
        logger.info("Initializing Iceberg S3 writer...")
        self._init_s3_config(config)
        self._init_catalog(config)
        logger.info(f"Initialized IcebergWriter with endpoint {self.endpoint_url}")

    def _init_s3_config(self, config: WriterConfig) -> None:
        """Initialize S3 configuration"""
        # Format endpoint URL
        self.endpoint_url = config.endpoint or 'http://localhost:9000'
        
        # Ensure s3_path starts with s3://
        if not config.s3_path.startswith('s3://'):
            self.s3_path = f"s3://{config.s3_path}"
        else:
            self.s3_path = config.s3_path
            
        logger.info(f"Using S3 path: {self.s3_path}")
        
        self.region = config.region or 'us-east-1'
        self.database = config.database or 'blockchain'
        self.anchor_table = config.anchor_table or 'blocks'

    def _init_catalog(self, config: WriterConfig) -> None:
        """Initialize Iceberg catalog"""
        # Format endpoint URL - ensure it's accessible
        self.endpoint_url = config.endpoint or 'http://minio:9000'  # Use minio service name
        
        # Create boto3 session for S3 access
        self.session = boto3.Session(
            region_name=self.region,
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )

        # Configure PyIceberg catalog using REST
        from pyiceberg.catalog import load_catalog
        self.catalog = load_catalog(
            "rest",  # Catalog name
            type="rest",  # Use REST catalog type
            uri="http://iceberg-rest:8181",  # Use iceberg-rest service name
            s3=dict(
                endpoint="http://minio:9000",  # Use minio service name
                access_key_id="minioadmin",
                secret_access_key="minioadmin",
                region=self.region,
                path_style_access="true",
            ),
            warehouse=self.s3_path,
            region=self.region,
        )
        # Create namespace if it doesn't exist
        try:
            self.catalog.create_namespace(self.database)
            logger.info(f"Created new namespace: {self.database}")
        except Exception as e:
            if "NamespaceAlreadyExistsError" in str(e.__class__.__name__):
                logger.info(f"Using existing namespace: {self.database}")
            else:
                logger.error(f"Error creating namespace: {e}")
                raise

        logger.info(f"Initialized Iceberg catalog with warehouse: {self.s3_path}")

        # Configure S3 client with proper settings
        self.s3_client = self.session.client(
            's3',
            endpoint_url=self.endpoint_url,  # Use minio service name
            region_name=self.region,
            config=boto3.session.Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'}
            ),
            use_ssl=False,  # MinIO doesn't use SSL in our setup
            verify=False,   # Skip SSL verification
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )

    async def write_iceberg(self, table_name: str, record_batch: pa.RecordBatch) -> None:
        """Write data to Iceberg table"""
        def write():
            try:
                # Convert PyArrow to Pandas for easier manipulation
                table = pa.Table.from_batches([record_batch])
                pandas_df = table.to_pandas()

                # Fully qualified table name
                namespace = self.database.replace('-', '_')  # Iceberg doesn't allow hyphens in namespace
                table_name_clean = table_name.replace('-', '_')  # Clean table name too
                full_table_name = f"{namespace}.{table_name_clean}"

                # Create or load table
                try:
                    iceberg_table = self.catalog.load_table(full_table_name)
                    logger.info(f"Loaded existing table: {full_table_name}")
                except Exception as e:
                    if "NoSuchTableError" not in str(e.__class__.__name__):
                        logger.error(f"Error loading table: {e}")
                        raise
                    logger.info(f"Creating new table: {full_table_name}")
                    # Create table if it doesn't exist
                    schema = _convert_pyarrow_to_iceberg_schema(record_batch)
                    
                    # Add table properties for S3 configuration
                    properties = {
                        'write.object-storage.path': f"{self.s3_path}/{table_name_clean}",
                        'write.object-storage.s3.endpoint': self.endpoint_url,
                        'write.object-storage.s3.access-key-id': 'minioadmin',
                        'write.object-storage.s3.secret-access-key': 'minioadmin',
                        'write.object-storage.s3.path-style-access': 'true',
                        'write.object-storage.s3.signer-type': 'S3SignerType',
                        'write.object-storage.s3.region': self.region
                    }

                    iceberg_table = self.catalog.create_table(
                        identifier=full_table_name, 
                        schema=schema,
                        location=f"{self.s3_path}/{table_name_clean}",
                        properties=properties
                    )

                # Write data to Iceberg table using PyArrow
                file_io = PyArrowFileIO(s3_client=self.s3_client)
                iceberg_table.append(pandas_df, file_io=file_io)

                logger.info(f"Successfully wrote {len(pandas_df)} rows to Iceberg table {full_table_name}")

            except Exception as e:
                logger.error(f"Failed to write to Iceberg table: {e}")
                raise

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, write)

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Push data to Iceberg tables"""
        try:
            logger.info(f"Starting push_data for {len(data)} tables: {', '.join(data.keys())}")

            # Separate anchor table from other tables
            if self.anchor_table is None:
                await self._write_tables(data)
            else:
                non_anchor_data = {k: v for k, v in data.items() if k != self.anchor_table}
                anchor_data = {k: v for k, v in data.items() if k == self.anchor_table}
                
                # Write non-anchor tables first
                if non_anchor_data:
                    await self._write_tables(non_anchor_data)
                
                # Write anchor table last
                if anchor_data:
                    await self._write_tables(anchor_data)
            
        except Exception as e:
            logger.error(f"Error writing to Iceberg: {e}")
            raise

    async def _write_tables(self, data: Dict[str, pa.RecordBatch]) -> None:
        """Write multiple tables concurrently"""
        table_tasks = {
            name: asyncio.create_task(self.write_iceberg(name, df), name=f"write_{name}")
            for name, df in data.items()
        }
        
        for name, task in table_tasks.items():
            try:
                await task
            except Exception as e:
                logger.error(f"Failed to write {name}: {str(e)}")
                raise Exception(f"Error writing Iceberg table {name}: {e}")