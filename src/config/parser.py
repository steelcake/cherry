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

class DataType(str, Enum):
    """Data type enum"""
    UINT64 = "uint64"
    UINT32 = "uint32"
    INT64 = "int64"
    INT32 = "int32"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    INTSTR = "intstr"
    STRING = "String"

class StreamState(BaseModel):
    """Stream state configuration"""
    path: str
    resume: bool = True
    last_block: Optional[int] = None

class ColumnCast(BaseModel):
    """Column casting configuration"""
    transaction: Optional[Dict[str, str]] = None
    value: Optional[str] = None
    amount: Optional[str] = None

class HexEncode(BaseModel):
    """Hex encoding configuration"""
    transaction: Optional[str] = None
    block: Optional[str] = None
    log: Optional[str] = None

class BlockRange(BaseModel):
    """Block range configuration"""
    from_block: int
    to_block: Optional[int] = None

class Stream(BaseModel):
    """Stream configuration"""
    kind: str
    name: Optional[str] = None
    signature: Optional[str] = None
    from_block: Optional[int] = None
    to_block: Optional[int] = None
    batch_size: Optional[int] = None
    include_transactions: Optional[bool] = False
    include_logs: Optional[bool] = False
    include_blocks: Optional[bool] = False
    include_traces: Optional[bool] = False
    topics: Optional[List[str]] = []
    address: Optional[List[str]] = []
    column_cast: Optional[ColumnCast] = None
    hex_encode: Optional[HexEncode] = None
    hash: Optional[List[str]] = []
    mapping: str
    state: Optional[StreamState] = None

class DataSource(BaseModel):
    """Data source configuration"""
    kind: str
    url: str
    token: str

class Output(BaseModel):
    """Output configuration"""
    kind: str
    path: Optional[str] = None
    endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    bucket: Optional[str] = None
    secure: Optional[bool] = False
    region: Optional[str] = None
    compression: Optional[str] = None
    batch_size: Optional[int] = None

class ProcessingConfig(BaseModel):
    """Processing configuration"""
    default_batch_size: int = 100000
    parallel_streams: bool = True
    max_retries: int = 3
    retry_delay: int = 5

class ContractIdentifier(BaseModel):
    """Contract identifier configuration"""
    name: str
    signature: str

class ContractConfig(BaseModel):
    """Contract configuration"""
    identifier_signatures: List[ContractIdentifier] = []

class Config(BaseModel):
    """Main configuration"""
    project_name: str
    description: str
    data_source: List[DataSource]
    streams: List[Stream]
    output: List[Output]
    processing: Optional[ProcessingConfig] = ProcessingConfig()
    contracts: ContractConfig
    blocks: Optional[BlockRange] = None
    transform: Optional[List[Dict]] = None

def parse_config(config_path: str) -> Config:
    """Parse configuration from YAML file"""
    with open(config_path, 'r') as f:
        raw_config = yaml.safe_load(f)
        
        # Convert project-name to project_name if needed
        if 'project-name' in raw_config:
            raw_config['project_name'] = raw_config.pop('project-name')
            
        # Convert blocks.range to blocks if needed
        if 'blocks' in raw_config and 'range' in raw_config['blocks']:
            raw_config['blocks'] = raw_config['blocks']['range']
    
    return Config(**raw_config)
