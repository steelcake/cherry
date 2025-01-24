from typing import List, Dict, Optional
from pathlib import Path
from pydantic import BaseModel, Field
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

class BlockRange(BaseModel):
    from_block: int
    to_block: Optional[int] = None

class BlockConfig(BaseModel):
    index_blocks: bool
    include_transactions: bool
    range: BlockRange
    contract_discovery: BlockRange

class TransactionFilters(BaseModel):
    addresses: Optional[List[str]] = None
    from_: Optional[List[str]] = Field(None, alias="from")
    to: Optional[List[str]] = None

class ContractIdentifier(BaseModel):
    name: str
    signature: str
    description: Optional[str] = None

class ContractConfig(BaseModel):
    identifier_signatures: List[ContractIdentifier]

class Event(BaseModel):
    name: str
    description: Optional[str] = None
    signature: str
    column_mapping: Dict[str, hypersync.DataType]
    filters: Optional[dict] = None

class ProcessingConfig(BaseModel):
    items_per_batch: int
    parallel_events: bool = True

class OutputConfig(BaseModel):
    enabled: bool = True
    url: Optional[str] = None
    output_dir: Optional[str] = None
    endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    bucket: Optional[str] = None
    secure: Optional[bool] = True
    region: Optional[str] = None
    batch_size: Optional[int] = None
    compression: Optional[str] = None

class Transform(BaseModel):
    kind: TransformKind

class Config(BaseModel):
    name: str
    description: Optional[str] = None
    data_source: List[DataSource]
    blocks: BlockConfig
    transactions: Optional[TransactionFilters] = None
    contracts: ContractConfig
    events: List[Event]
    processing: ProcessingConfig
    transform: List[Transform]
    output: Dict[str, OutputConfig]

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
