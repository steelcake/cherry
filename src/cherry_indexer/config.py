from dataclasses import dataclass
import clickhouse_connect
from typing import List, Dict, Optional
from enum import Enum
import logging
from cherry_core.ingest import ProviderConfig 
import pyiceberg

logger = logging.getLogger(__name__)

class WriterKind(str, Enum):
    CLICKHOUSE = "clickhouse"
    ICEBERG = "iceberg"

class StepKind(str, Enum):
    EVM_VALIDATE_BLOCK = 'evm_validate_block_data'
    EVM_DECODE_EVENTS = 'evm_decode_events'
    CAST = 'cast'

@dataclass
class Provider:
    """Data provider configuration"""
    name: Optional[str] = None
    config: Optional[ProviderConfig] = None

@dataclass
class IcebergWriterConfig:
    namespace: str
    database: str 
    catalog: pyiceberg.Catalog 

@dataclass
class ClickHouseWriterConfig:
    client: clickhouse_connect.driver.client.Client

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

