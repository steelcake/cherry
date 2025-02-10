from typing import List, Dict, Optional, Union, Any
from pathlib import Path
from pydantic import BaseModel, Field, model_validator
from enum import Enum
import yaml, logging, json
from src.utils.logging_setup import setup_logging
import hypersync
import os
from hypersync import DataType
from dataclasses import dataclass

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
    AWS_WRANGLER_S3 = "AWSWranglerS3Writer"
    CLICKHOUSE = "Clickhouse"
    
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

class ColumnCastField(BaseModel):
    """Column casting field configuration"""
    value: Optional[str] = None
    block_number: Optional[str] = None

class ColumnCast(BaseModel):
    """Column casting configuration"""
    transaction: Optional[Dict[str, str]] = None
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
    topics: Optional[List[Optional[Union[str, List[str]]]]] = None
    address: Optional[List[str]] = None
    column_cast: Optional[Dict[str, Union[str, ColumnCastField]]] = None
    hex_encode: Optional[HexEncode] = None
    hash: Optional[List[str]] = []
    mapping: Optional[str] = None
    state: Optional[StreamState] = None

    @model_validator(mode='before')
    def convert_state_dict(cls, values):
        """Convert state dict to StreamState if needed"""
        if isinstance(values.get('state'), dict):
            values['state'] = StreamState(**values['state'])
        return values

class DataSource(BaseModel):
    """Data source configuration"""
    kind: str
    url: str
    token: str

@dataclass
class Output:
    kind: str
    # S3 fields
    endpoint: Optional[str] = None
    bucket: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    region: Optional[str] = None
    secure: Optional[bool] = None
    # Parquet fields
    output_path: Optional[str] = None
    # Common fields
    batch_size: Optional[int] = None
    compression: Optional[str] = None
    # Database fields
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    # AWS Wrangler S3 fields
    s3_path: Optional[str] = None
    partition_cols: Optional[Dict[str, List[str]]] = None
    default_partition_cols: Optional[List[str]] = None

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

@dataclass
class Config:
    project_name: str
    description: str
    data_source: List[DataSource]
    streams: List[Stream]
    output: List[Output]
    contracts: Optional[dict] = None

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
        
        # Convert raw dicts to proper objects
        if 'data_source' in raw_config:
            raw_config['data_source'] = [DataSource(**ds) for ds in raw_config['data_source']]
        
        if 'streams' in raw_config:
            raw_config['streams'] = [Stream(**s) for s in raw_config['streams']]
        
        if 'output' in raw_config:
            raw_config['output'] = [Output(**o) for o in raw_config['output']]
    
    return Config(**raw_config)
