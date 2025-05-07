import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Callable

from cherry_core.ingest import ProviderConfig, Query
from cherry_core.svm_decode import InstructionSignature, LogSignature
from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
from pyiceberg.catalog import Catalog as IcebergCatalog
import deltalake
import pyarrow as pa
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
    U256_TO_BINARY = "u256_to_binary"
    SVM_DECODE_INSTRUCTIONS = "svm_decode_instructions"
    SVM_DECODE_LOGS = "svm_decode_logs"
    JOIN_BLOCK_DATA = "join_block_data"
    JOIN_SVM_TRANSACTION_DATA = "join_svm_transaction_data"
    JOIN_EVM_TRANSACTION_DATA = "join_evm_transaction_data"
    GLACIERS_EVENTS = "glaciers_events"


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
    engine: str = "MergeTree()"
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
class JoinBlockDataConfig:
    tables: Optional[list[str]] = None
    join_left_on: list[str] = field(default_factory=lambda: ["block_number"])
    join_blocks_on: list[str] = field(default_factory=lambda: ["number"])


@dataclass
class JoinSvmTransactionDataConfig:
    tables: Optional[list[str]] = None
    join_left_on: list[str] = field(
        default_factory=lambda: ["block_slot", "transaction_index"]
    )
    join_transactions_on: list[str] = field(
        default_factory=lambda: ["block_slot", "transaction_index"]
    )


@dataclass
class JoinEvmTransactionDataConfig:
    tables: Optional[list[str]] = None
    join_left_on: list[str] = field(
        default_factory=lambda: ["block_number", "transaction_index"]
    )
    join_transactions_on: list[str] = field(
        default_factory=lambda: ["block_number", "transaction_index"]
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
class GlaciersEventsConfig:
    abi_db_path: str
    decoder_type: str = "log"
    input_table: str = "logs"
    output_table: str = "decoded_logs"


@dataclass
class SvmDecodeInstructionsConfig:
    instruction_signature: InstructionSignature
    allow_decode_fail: bool = False
    input_table: str = "instructions"
    output_table: str = "decoded_instructions"
    hstack: bool = True


@dataclass
class SvmDecodeLogsConfig:
    log_signature: LogSignature
    allow_decode_fail: bool = False
    input_table: str = "logs"
    output_table: str = "decoded_logs"
    hstack: bool = True


@dataclass
class CastConfig:
    table_name: str
    mappings: Dict[str, pa.DataType]
    allow_cast_fail: bool = False


@dataclass
class HexEncodeConfig:
    tables: Optional[list[str]] = None
    prefixed: bool = True


@dataclass
class U256ToBinaryConfig:
    tables: Optional[list[str]] = None


@dataclass
class Base58EncodeConfig:
    tables: Optional[list[str]] = None


@dataclass
class CastByTypeConfig:
    from_type: pa.DataType
    to_type: pa.DataType
    allow_cast_fail: bool = False


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
        | U256ToBinaryConfig
        | CastByTypeConfig
        | Base58EncodeConfig
        | SvmDecodeInstructionsConfig
        | SvmDecodeLogsConfig
        | CustomStepConfig
        | JoinBlockDataConfig
        | JoinSvmTransactionDataConfig
        | JoinEvmTransactionDataConfig
        | GlaciersEventsConfig
    )
    name: Optional[str] = None


@dataclass
class Pipeline:
    provider: ProviderConfig
    query: Query
    writer: Writer
    steps: List[Step]
