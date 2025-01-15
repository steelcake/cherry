from typing import List, Dict, Optional, Union
from pathlib import Path
from pydantic import BaseModel, RootModel
import yaml, logging, json
from enum import Enum
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

class PostgresOutput(BaseModel):
    kind: OutputKind = OutputKind.POSTGRES
    url: str

class ParquetOutput(BaseModel):
    kind: OutputKind = OutputKind.PARQUET
    output_dir: str

class Output(RootModel):
    root: List[Union[PostgresOutput, ParquetOutput]]

class Config(BaseModel):
    name: str
    data_source: List[DataSource]
    blocks: Optional[BlockConfig] = None
    transactions: Optional[TransactionFilters] = None
    events: List[Event]
    contract_identifier_signatures: Optional[List[str]] = None
    items_per_section: int
    parquet_output_path: str
    from_block: int
    to_block: Optional[int]
    transform: List[Transform]
    output: List[Union[PostgresOutput, ParquetOutput]]

def parse_config(config_path: Path) -> Config:
    """Parse configuration from YAML file"""
    logger.info(f"Parsing configuration from {config_path}")
    try:
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
            logger.debug(f"Config dict: {config_dict}")
        
        config = Config.model_validate(config_dict)
        
        # Log detailed configuration
        logger.debug("Parsed configuration details:")
        logger.debug(f"Project name: {config.name}")
        logger.debug(f"Data sources: {json.dumps([ds.model_dump() for ds in config.data_source], indent=2)}")
        if config.blocks:
            logger.debug(f"Block config: {json.dumps(config.blocks.model_dump(), indent=2)}")
        if config.transactions:
            logger.debug(f"Transaction filters: {json.dumps(config.transactions.model_dump(), indent=2)}")
        logger.debug(f"Events config: {json.dumps([event.model_dump() for event in config.events], indent=2)}")
        logger.debug(f"Transform config: {json.dumps([t.model_dump() for t in config.transform], indent=2)}")
        logger.debug(f"Output config: {json.dumps([o.model_dump() for o in config.output], indent=2)}")
        
        return config
    except Exception as e:
        logger.error(f"Error parsing configuration: {e}")
        logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
        raise

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    try:
        config = parse_config(Path("config.yaml"))
        logger.info("Configuration parsed successfully")
        print(config.model_dump_json(indent=2))
    except Exception as e:
        logger.error(f"Failed to parse configuration: {e}")
