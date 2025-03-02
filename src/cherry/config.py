from dataclasses import dataclass
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
    EVM_VALIDATE_BLOCK = "evm_validate_block_data"
    EVM_DECODE_EVENTS = "evm_decode_events"
    CAST = "cast"
    HEX_ENCODE = "hex_encode"


@dataclass
class Provider:
    """Data provider configuration"""

    name: Optional[str] = None
    config: Optional[ProviderConfig] = None


@dataclass
class IcebergWriterConfig:
    namespace: str
    catalog: IcebergCatalog
    write_location: str


@dataclass
class ClickHouseWriterConfig:
    client: ClickHouseClient


@dataclass
class Writer:
    kind: WriterKind
    config: ClickHouseWriterConfig | IcebergWriterConfig


@dataclass
class Step:
    name: str
    kind: StepKind | str
    config: Optional[Dict] = None


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
