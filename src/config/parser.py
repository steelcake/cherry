from typing import List, Dict, Optional, Union
from pathlib import Path
from pydantic import BaseModel, Field
from enum import Enum
import yaml, logging, json
from src.utils.logging_setup import setup_logging
import hypersync
import os
from hypersync import DataType

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
    kind: str
    url: str
    api_key: Optional[str] = None

class BlockRange(BaseModel):
    """Block range configuration"""
    from_block: int
    to_block: Optional[int] = None

class BlockConfig(BaseModel):
    """Block processing configuration"""
    range: BlockRange
    contract_discovery: BlockRange
    index_blocks: bool = True
    include_transactions: bool = False

class TransactionFilters(BaseModel):
    addresses: Optional[List[str]] = None
    from_: Optional[List[str]] = Field(None, alias="from")
    to: Optional[List[str]] = None

class ContractIdentifier(BaseModel):
    """Contract identifier configuration"""
    name: str
    signature: str
    description: Optional[str] = None

class Event(BaseModel):
    """Event configuration"""
    name: str
    signature: str
    description: Optional[str] = None
    column_mapping: Optional[Dict[str, DataType]] = None
    filters: Optional[Dict[str, List[str]]] = None

class ProcessingConfig(BaseModel):
    """Processing configuration"""
    items_per_batch: int = 30000
    parallel_events: bool = True

class OutputConfig(BaseModel):
    """Output configuration"""
    postgres: Optional[Dict] = None
    parquet: Optional[Dict] = None
    s3: Optional[Dict] = None

class Transform(BaseModel):
    kind: TransformKind

class ContractConfig(BaseModel):
    """Contract configuration"""
    identifier_signatures: List[ContractIdentifier] = []

class Config(BaseModel):
    """Main configuration"""
    name: str
    description: str
    data_source: List[DataSource]
    blocks: BlockConfig
    events: List[Event]
    contracts: ContractConfig
    processing: ProcessingConfig
    output: OutputConfig
    transactions: Optional[Dict] = None
    transform: Optional[List[Dict]] = None

def parse_config(config_path: Union[str, Path]) -> Config:
    """Parse configuration from YAML file"""
    logger.info(f"Parsing configuration from {config_path}")
    
    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    # Replace environment variables
    for source in raw_config.get('data_source', []):
        if 'api_key' in source and source['api_key'].startswith('${'):
            env_var = source['api_key'][2:-1]
            source['api_key'] = os.environ.get(env_var)

    # Convert block ranges
    blocks = raw_config.get('blocks', {})
    blocks['range'] = BlockRange(**blocks.get('range', {}))
    blocks['contract_discovery'] = BlockRange(**blocks.get('contract_discovery', {}))

    # Convert contracts config
    contracts_data = raw_config.get('contracts', {})
    if isinstance(contracts_data, dict):
        contracts = ContractConfig(
            identifier_signatures=[
                ContractIdentifier(**sig) 
                for sig in contracts_data.get('identifier_signatures', [])
            ]
        )
    else:
        contracts = ContractConfig()

    # Create and validate config
    return Config(
        name=raw_config.get('name', ''),
        description=raw_config.get('description', ''),
        data_source=[DataSource(**src) for src in raw_config.get('data_source', [])],
        blocks=BlockConfig(**blocks),
        events=[Event(**event) for event in raw_config.get('events', [])],
        contracts=contracts,
        processing=ProcessingConfig(**raw_config.get('processing', {})),
        output=OutputConfig(**raw_config.get('output', {})),
        transactions=raw_config.get('transactions'),
        transform=raw_config.get('transform')
    )
