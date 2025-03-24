import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Callable

from cherry_core.ingest import ProviderConfig
from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
from pyiceberg.catalog import Catalog as IcebergCatalog
import deltalake
import pyarrow as pa
import pyarrow.compute as pa_compute
import pyarrow.dataset as pa_dataset
import pyarrow.fs as pa_fs
import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class WriterKind(str, Enum):
    CLICKHOUSE = "clickhouse"
    ICEBERG = "iceberg"
    DELTA_LAKE = "delta_lake"
    PYARROW_DATASET = "pyarrow_dataset"
    DUCKDB = "duckdb"


class StepKind(str, Enum):
    CUSTOM = "custom"
    EVM_VALIDATE_BLOCK_DATA = "evm_validate_block_data"
    EVM_DECODE_EVENTS = "evm_decode_events"
    CAST = "cast"
    HEX_ENCODE = "hex_encode"
    CAST_BY_TYPE = "cast_by_type"
    BASE58_ENCODE = "base58_encode"


@dataclass
class IcebergWriterConfig:
    namespace: str
    catalog: IcebergCatalog
    write_location: str


@dataclass
class DeltaLakeWriterConfig:
    data_uri: str
    partition_by: Dict[str, list[str]] = field(default_factory=dict)
    storage_options: Optional[Dict[str, str]] = None
    writer_properties: Optional[deltalake.WriterProperties] = None
    anchor_table: Optional[str] = None


@dataclass
class ClickHouseSkipIndex:
    name: str
    val: str
    type_: str
    granularity: int


@dataclass
class ClickHouseWriterConfig:
    client: ClickHouseClient
    codec: Dict[str, Dict[str, str]] = field(default_factory=dict)
    order_by: Dict[str, List[str]] = field(default_factory=dict)
    skip_index: Dict[str, List[ClickHouseSkipIndex]] = field(default_factory=dict)
    anchor_table: Optional[str] = None


@dataclass
class PyArrowDatasetWriterConfig:
    base_dir: str
    basename_template: Optional[str] = None
    partitioning: Dict[str, pa_dataset.Partitioning | list[str]] = field(
        default_factory=dict
    )
    partitioning_flavor: Dict[str, str] = field(default_factory=dict)
    filesystem: Optional[pa_fs.FileSystem] = None
    file_options: Optional[pa_dataset.FileWriteOptions] = None
    use_threads: bool = True
    max_partitions: int = 1024
    max_open_files: int = 1024
    max_rows_per_file: int = 0
    min_rows_per_group: int = 0
    max_rows_per_group: int = 1024 * 1024
    create_dir: bool = True
    anchor_table: Optional[str] = None


@dataclass
class DuckdbWriterConfig:
    connection: duckdb.DuckDBPyConnection


@dataclass
class Writer:
    kind: WriterKind
    config: (
        ClickHouseWriterConfig
        | IcebergWriterConfig
        | DeltaLakeWriterConfig
        | PyArrowDatasetWriterConfig
        | DuckdbWriterConfig
    )


@dataclass
class EvmValidateBlockDataConfig:
    blocks: str = "blocks"
    transactions: str = "transactions"
    logs: str = "logs"
    traces: str = "traces"


@dataclass
class EvmDecodeEventsConfig:
    event_signature: str
    allow_decode_fail: bool = False
    input_table: str = "logs"
    output_table: str = "decoded_logs"
    hstack: bool = True


@dataclass
class CastConfig:
    table_name: str
    mappings: Dict[str, pa.DataType]
    safe: Optional[bool] = None
    options: Optional[pa_compute.CastOptions] = None


@dataclass
class HexEncodeConfig:
    tables: Optional[list[str]] = None
    prefixed: bool = True


@dataclass
class Base58EncodeConfig:
    tables: Optional[list[str]] = None


@dataclass
class CastByTypeConfig:
    from_type: pa.DataType
    to_type: pa.DataType
    safe: Optional[bool] = None
    options: Optional[pa_compute.CastOptions] = None


@dataclass
class CustomStepConfig:
    runner: Callable[[Dict[str, pl.DataFrame], Optional[Any]], Dict[str, pl.DataFrame]]
    context: Optional[Any] = None


@dataclass
class Step:
    kind: StepKind
    config: (
        EvmValidateBlockDataConfig
        | EvmDecodeEventsConfig
        | CastConfig
        | HexEncodeConfig
        | CastByTypeConfig
        | Base58EncodeConfig
        | CustomStepConfig
    )
    name: Optional[str] = None


@dataclass
class Pipeline:
    provider: ProviderConfig
    writer: Writer
    steps: List[Step]
