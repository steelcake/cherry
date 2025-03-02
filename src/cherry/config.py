from dataclasses import dataclass, field
from cherry_core import Tuple
from clickhouse_connect.driver.client import Client as ClickHouseClient
from typing import List, Dict, Optional
from enum import Enum
import logging
from cherry_core.ingest import ProviderConfig
from pyiceberg.catalog import Catalog as IcebergCatalog

logger = logging.getLogger(__name__)


class WriterKind(str, Enum):
    CLICKHOUSE = "clickhouse"
    ICEBERG = "iceberg"


class StepKind(str, Enum):
    EVM_VALIDATE_BLOCK_DATA = "evm_validate_block_data"
    EVM_DECODE_EVENTS = "evm_decode_events"
    CAST = "cast"
    HEX_ENCODE = "hex_encode"

@dataclass
class Provider:
    """Data provider configuration"""

    config: ProviderConfig
    name: Optional[str] = None


@dataclass
class IcebergWriterConfig:
    namespace: str
    catalog: IcebergCatalog
    write_location: str


@dataclass
class ClickHouseWriterConfig:
    client: ClickHouseClient
    order_by: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class Writer:
    kind: WriterKind
    config: ClickHouseWriterConfig | IcebergWriterConfig


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


@dataclass
class CastConfig:
    table_name: str
    mappings: List[Tuple[str, str]]
    allow_cast_fail: bool = False


@dataclass
class HexEncodeConfig:
    tables: Optional[list[str]] = None
    prefixed: bool = True


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
