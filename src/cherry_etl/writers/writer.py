from . import pyarrow_dataset
from ..writers.base import DataWriter
from ..config import (
    Writer,
    WriterKind,
    IcebergWriterConfig,
    ClickHouseWriterConfig,
    DeltaLakeWriterConfig,
    PyArrowDatasetWriterConfig,
    DuckdbWriterConfig,
)
from ..writers import iceberg, clickhouse, delta_lake, duckdb
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
        case WriterKind.DELTA_LAKE:
            assert isinstance(writer.config, DeltaLakeWriterConfig)
            return delta_lake.Writer(writer.config)
        case WriterKind.PYARROW_DATASET:
            assert isinstance(writer.config, PyArrowDatasetWriterConfig)
            return pyarrow_dataset.Writer(writer.config)
        case WriterKind.DUCKDB:
            assert isinstance(writer.config, DuckdbWriterConfig)
            return duckdb.Writer(writer.config)
        case _:
            raise ValueError(f"Invalid writer kind: {writer.kind}")
