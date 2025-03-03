import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union

from cherry_core import Tuple
from cherry_core.ingest import ProviderConfig
from clickhouse_connect.driver.asyncclient import AsyncClient as ClickHouseClient
from deltalake import DataCatalog
from pyiceberg.catalog import Catalog as IcebergCatalog

logger = logging.getLogger(__name__)


class WriterKind(str, Enum):
    CLICKHOUSE = "clickhouse"
    ICEBERG = "iceberg"
    DELTA_LAKE = "delta_lake"


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
class DeltaLakeWriterConfig:
    table_uri: Optional[str] = None

    # New catalog-based config using the enum type
    data_catalog: Optional[Union[str, "DataCatalog"]] = (
        None  # To support both string and enum
    )
    database_name: Optional[str] = None
    table_name: Optional[str] = None
    catalog_options: Optional[Dict[str, str]] = field(default_factory=dict)

    # Storage options for both approaches
    storage_options: Optional[Dict[str, str]] = field(default_factory=dict)

    def __post_init__(self):
        """Validate configuration."""
        if not self.table_uri and not (self.data_catalog and self.database_name):
            raise ValueError(
                "Either table_uri or (data_catalog and database_name) must be provided"
            )


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
    hstack: bool = True


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
