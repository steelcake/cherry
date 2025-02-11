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

@dataclass
class DataSource:
    """Data source configuration"""
    url: str
    token: str

@dataclass
class Stream:
    """Stream configuration"""
    kind: str  # 'block' or 'event'
    name: Optional[str] = None  # Required for events
    signature: Optional[str] = None  # Required for events
    from_block: int = 0
    to_block: Optional[int] = None
    batch_size: int = 10000
    addresses: List[str] = None
    include_blocks: bool = True
    state_path: Optional[str] = None
    resume: bool = False

@dataclass
class Output:
    """Output configuration"""
    kind: str
    # Common settings
    output_path: Optional[str] = None
    compression: Optional[str] = None
    batch_size: int = 10000
    
    # S3 settings
    endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    bucket: Optional[str] = None
    s3_path: Optional[str] = None
    region: Optional[str] = None
    secure: bool = True
    
    # Database settings
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

@dataclass
class Config:
    """Main configuration"""
    data_source: List[DataSource]
    streams: List[Stream]
    output: List[Output]

def _parse_stream(data: Dict[str, Any]) -> Stream:
    """Parse stream configuration"""
    if data['kind'] == 'block':
        return Stream(
            kind='block',
            from_block=data.get('from_block', 0),
            to_block=data.get('to_block'),
            batch_size=data.get('batch_size', 10000),
            state_path=data.get('state', {}).get('path'),
            resume=data.get('state', {}).get('resume', False)
        )
    else:
        return Stream(
            kind='event',
            name=data['name'],
            signature=data['signature'],
            from_block=data.get('from_block', 0),
            to_block=data.get('to_block'),
            batch_size=data.get('batch_size', 10000),
            addresses=data.get('address', []),
            include_blocks=data.get('include_blocks', True),
            state_path=data.get('state', {}).get('path'),
            resume=data.get('state', {}).get('resume', False)
        )

def _parse_output(data: Dict[str, Any]) -> Output:
    """Parse output configuration"""
    # Replace environment variables in sensitive fields
    for key in ['access_key', 'secret_key', 'password']:
        if key in data and data[key] and isinstance(data[key], str):
            data[key] = os.path.expandvars(data[key])
    
    return Output(**data)

def parse_config(path: str) -> Config:
    """Parse configuration file"""
    with open(path) as f:
        data = yaml.safe_load(f)
    
    return Config(
        data_source=[
            DataSource(
                url=src['url'],
                token=os.path.expandvars(src['token'])
            )
            for src in data['data_source']
        ],
        streams=[_parse_stream(s) for s in data['streams']],
        output=[_parse_output(o) for o in data['output']]
    )
