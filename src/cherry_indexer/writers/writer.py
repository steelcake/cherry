from ..writers.base import DataWriter
from ..config import Writer, WriterKind, IcebergWriterConfig, ClickHouseWriterConfig
from ..writers import iceberg, clickhouse
import logging

logger = logging.getLogger(__name__)


def create_writer(writer: Writer) -> DataWriter:
    match writer.kind:
        case WriterKind.ICEBERG:
            assert isinstance(writer.config, IcebergWriterConfig)
            return iceberg.Writer(writer.config)
        case WriterKind.CLICKHOUSE:
            assert isinstance(writer.config, ClickHouseWriterConfig)
            return clickhouse.Writer(writer.config)
        case _:
            raise ValueError(f"Invalid writer kind: {writer.kind}")
