from typing import List, Dict, Optional
from pathlib import Path
from pydantic import BaseModel
from enum import Enum
import yaml, logging, json
from src.utils.logging_setup import setup_logging
import hypersync

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

class DataSourceKind(str, Enum):
    ETH_RPC = "eth_rpc"
    HYPERSYNC = "hypersync"

class TransformKind(str, Enum):
    POLARS = "Polars"
    PANDAS = "Pandas"

class OutputKind(str, Enum):
    POSTGRES = "Postgres"
    DUCKDB = "Duckdb"
    PARQUET = "Parquet"
    S3 = "S3"

class DataSource(BaseModel):
    kind: DataSourceKind
    url: str
    api_key: Optional[str] = None

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
    column_mapping: Dict[str, hypersync.DataType]

class Transform(BaseModel):
    kind: TransformKind

class Output(BaseModel):
    enabled: bool = True
    kind: OutputKind
    url: Optional[str] = None
    output_dir: Optional[str] = None
    # S3 specific fields
    endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    bucket: Optional[str] = None
    secure: Optional[bool] = True

class Config(BaseModel):
    name: str
    data_source: List[DataSource]
    blocks: Optional[BlockConfig] = None
    transactions: Optional[TransactionFilters] = None
    events: List[Event]
    contract_identifier_signatures: Optional[List[str]] = None
    items_per_section: int
    from_block: int
    to_block: Optional[int]
    transform: List[Transform]
    output: List[Output]

def parse_config(config_path: Path) -> Config:
    """Parse configuration from YAML file"""
    logger.info(f"Parsing configuration from {config_path}")
    try:
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
            logger.debug(f"Config dict: {config_dict}")
        
        config = Config.model_validate(config_dict)
        logger.debug(f"Parsed config: {json.dumps(config.model_dump(), indent=2)}")
        return config
    except Exception as e:
        logger.error(f"Error parsing configuration: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise
