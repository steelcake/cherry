import asyncio, logging
from typing import Dict
from src.types.data import Data
from src.writers.base import DataWriter
from src.writers.parquet import ParquetWriter
from src.writers.postgres import PostgresWriter
from src.writers.s3 import S3Writer
from src.writers.clickhouse import ClickHouseWriter
from sqlalchemy import create_engine
from src.config.parser import Output

logger = logging.getLogger(__name__)

class Writer:
    """Handles writing data to multiple targets"""
    @staticmethod
    def create_writer(kind: str, config: Output) -> DataWriter:
        """Create a writer instance based on configuration"""
        if kind == 'local_parquet':
            if not config.path:
                raise ValueError("path is required for local_parquet writer")
            return ParquetWriter(config.path)
        elif kind == 'postgres':
            if not config.connection_string:
                raise ValueError("connection_string is required for postgres writer")
            engine = create_engine(config.connection_string)
            return PostgresWriter(engine)
        elif kind == 's3':
            if not config.endpoint or not config.bucket:
                raise ValueError("endpoint and bucket are required for s3 writer")
            return S3Writer(
                endpoint=config.endpoint,
                bucket=config.bucket,
                access_key=config.access_key,
                secret_key=config.secret_key,
                region=config.region,
                secure=config.secure if config.secure is not None else True
            )
        elif kind == 'clickhouse':
            if not config.host:
                raise ValueError("host is required for clickhouse writer")
            return ClickHouseWriter(
                host=config.host,
                port=config.port or 8123,
                username=config.username or 'default',
                password=config.password or '',
                database=config.database or 'default',
                secure=config.secure if config.secure is not None else False
            )
        else:
            raise ValueError(f"Unknown writer kind: {kind}")

    def __init__(self, writers: Dict[str, DataWriter]):
        self._writers = writers
        logger.info(f"Initialized Writer with {len(writers)} writers: {', '.join(writers.keys())}")

    async def write(self, data: Data) -> None:
        """Write data to all configured targets"""
        if not self._writers:
            logger.error("No writers initialized")
            return

        if not data or not data.events:
            logger.debug("No data to write")
            return

        try:
            # Create separate copies for each writer
            writer_data = {}
            for writer_type in self._writers.keys():
                writer_data[writer_type] = Data(
                    events={name: df.clone() for name, df in data.events.items()} if data.events else None,
                    blocks={name: df.clone() for name, df in data.blocks.items()} if data.blocks else None,
                    transactions=data.transactions
                )

            # Write in parallel to all targets
            write_tasks = {
                writer_type: asyncio.create_task(
                    writer.write(writer_data[writer_type]),
                    name=f"write_{writer_type}"
                )
                for writer_type, writer in self._writers.items()
            }
            
            if write_tasks:
                logger.info(f"Writing in parallel to {len(write_tasks)} targets ({', '.join(self._writers.keys())})")
                
                # Wait for all writes to complete concurrently
                results = await asyncio.gather(*write_tasks.values(), return_exceptions=True)
                
                # Check for any errors
                for writer_type, result in zip(write_tasks.keys(), results):
                    if isinstance(result, Exception):
                        logger.error(f"Error in {writer_type} writer: {result}")
                        raise result

        except Exception as e:
            logger.error(f"Error during parallel write: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 