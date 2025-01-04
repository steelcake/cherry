from typing import List, Optional
from pathlib import Path
from pydantic import BaseModel
import yaml
from enum import Enum

class DataSourceKind(str, Enum):
    ETH_RPC = "eth_rpc"
    HYPERSYNC = "hypersync"

class TransformKind(str, Enum):
    POLARS = "Polars"
    PANDAS = "Pandas"

class OutputKind(str, Enum):
    POSTGRES = "Postgres"
    DUCKDB = "Duckdb"

class DataSource(BaseModel):
    kind: DataSourceKind
    url: str  # Keeps string type (connection URL)

class BlockConfig(BaseModel):
    index_blocks: bool
    include_transactions: bool

class TransactionFilters(BaseModel):
    from_address: Optional[List[str]] = None
    to_address: Optional[List[str]] = None

class Event(BaseModel):
    name: str
    address: Optional[List[str]] = None
    topics: Optional[List[List[str]]] = None
    signature: str

class Transform(BaseModel):
    kind: TransformKind

class Output(BaseModel):
    kind: OutputKind
    url: str

class Config(BaseModel):
    name: str
    data_source: List[DataSource]
    blocks: Optional[BlockConfig] = None
    transactions: Optional[TransactionFilters] = None
    events: List[Event]
    transform: List[Transform]
    output: List[Output]

def parse_config(config_path: Path) -> Config:
    with open(config_path, 'r') as f:
        yaml_data = yaml.safe_load(f)
    return Config.model_validate(yaml_data)

if __name__ == "__main__":
    config = parse_config(Path("config.yaml"))
    print(config.model_dump_json(indent=2))
