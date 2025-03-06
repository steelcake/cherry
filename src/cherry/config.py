import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Literal, TypeAlias

from cherry_core.ingest import ProviderConfig
from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
from pyiceberg.catalog import Catalog as IcebergCatalog
import deltalake
import pyarrow as pa
import pyarrow.compute as pa_compute
from pyarrow import fs

logger = logging.getLogger(__name__)


class WriterKind(str, Enum):
    CLICKHOUSE = "clickhouse"
    ICEBERG = "iceberg"
    DELTA_LAKE = "delta_lake"
    PYARROW_DATASET = "pyarrow_dataset"


class StepKind(str, Enum):
    EVM_VALIDATE_BLOCK_DATA = "evm_validate_block_data"
    EVM_DECODE_EVENTS = "evm_decode_events"
    CAST = "cast"
    HEX_ENCODE = "hex_encode"
    CAST_BY_TYPE = "cast_by_type"
    BASE58_ENCODE = "base58_encode"


@dataclass
class Provider:
    config: ProviderConfig
    name: Optional[str] = None


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


ExistingDataBehavior: TypeAlias = Literal[
    "overwrite_or_ignore", "error", "delete_matching"
]


@dataclass
class PyArrowDatasetWriterConfig:
    output_dir: str
    partition_cols: Optional[Dict[str, List[str]]] = None
    anchor_table: Optional[str] = None
    max_partitions: int = 100000
    filesystem: Optional[fs.FileSystem] = None
    use_threads: bool = True
    existing_data_behavior: ExistingDataBehavior = "overwrite_or_ignore"


@dataclass
class Writer:
    kind: WriterKind
    config: (
        ClickHouseWriterConfig
        | IcebergWriterConfig
        | DeltaLakeWriterConfig
        | PyArrowDatasetWriterConfig
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
class Step:
    name: str
    kind: StepKind | str
    config: Optional[
        Dict
        | EvmValidateBlockDataConfig
        | EvmDecodeEventsConfig
        | CastConfig
        | HexEncodeConfig
        | CastByTypeConfig
        | Base58EncodeConfig
    ] = None


@dataclass
class Pipeline:
    provider: Provider
    writer: Writer
    steps: List[Step]


@dataclass
class Config:
    project_name: str
    description: str
    pipelines: Dict[str, Pipeline]
