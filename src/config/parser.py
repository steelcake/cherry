from typing import List, Dict, Optional, Union, Any
from pathlib import Path
from pydantic import BaseModel, Field, model_validator
from enum import Enum
import yaml, logging
from src.utils.logging_setup import setup_logging
import os
from cherry_core.ingest import EvmQuery

logger = logging.getLogger(__name__)

class ProviderKind(str, Enum):
    SQD = "sqd"

class WriterKind(str, Enum):
    LOCAL_PARQUET = "local_parquet"
    AWS_WRANGLER_S3 = "aws_wrangler_s3"
    POSTGRES = "postgres"
    CLICKHOUSE = "clickhouse"

class StepKind(str, Enum):
    EVM_VALIDATE_BLOCK = "evm_validate_block_data"
    EVM_DECODE_EVENTS = "evm_decode_events"

class Format(str, Enum):
    EVM = "evm"

class ProviderConfig(BaseModel):
    """Provider-specific configuration"""
    url: Optional[str] = None
    format: Optional[Format] = None
    query: Optional[Dict] = None

class Provider(BaseModel):
    """Data provider configuration"""
    name: Optional[str] = None
    kind: Optional[ProviderKind] = None
    config: Optional[ProviderConfig] = None

class WriterConfig(BaseModel):
    """Writer-specific configuration"""
    path: Optional[str] = ""
    endpoint: Optional[str] = None
    database: Optional[str] = None
    use_boto3: Optional[bool] = None
    s3_path: Optional[str] = None
    region: Optional[str] = None
    anchor_table: Optional[str] = None
    partition_cols: Optional[Dict[str, List[str]]] = None
    default_partition_cols: Optional[List[str]] = None

class Writer(BaseModel):
    """Data writer configuration"""
    name: Optional[str] = None
    kind: Optional[WriterKind] = None
    config: Optional[WriterConfig] = None

class StepConfig(BaseModel):
    """Step-specific configuration"""
    event_signature: Optional[str] = None
    input_table: Optional[str] = None
    output_table: Optional[str] = None
    allow_decode_fail: Optional[bool] = None

class Step(BaseModel):
    """Pipeline step configuration"""
    name: str
    kind: Optional[StepKind] = None
    config: Optional[StepConfig] = None

class Pipeline(BaseModel):
    """Data pipeline configuration"""
    name: Optional[str] = None
    provider: Provider
    steps: List[Step]
    writer: Writer

class Config(BaseModel):
    """Main configuration"""
    project_name: str
    description: str
    providers: Dict[str, Provider]
    writers: Dict[str, Writer]
    pipelines: Dict[str, Pipeline]

def parse_config(config_path: str) -> Config:
    """Parse configuration from YAML file"""
    try:
        with open(config_path, 'r') as f:
            raw_config = yaml.safe_load(f)
            
            # Parse configuration
            config = Config(**raw_config)
            
            logger.info(f"Loaded configuration for project: {config.project_name}")
            logger.info(f"Found {len(config.providers)} providers")
            logger.info(f"Found {len(config.writers)} writers")
            logger.info(f"Found {len(config.pipelines)} pipelines")
            
            return config
            
    except Exception as e:
        logger.error(f"Error parsing config file {config_path}: {e}")
        raise

def get_provider_config(config: Config, provider_name: str) -> Optional[Provider]:
    """Get provider configuration by name"""
    return next((p for p in config.providers if p.name == provider_name), None)

def get_writer_config(config: Config, writer_name: str) -> Optional[Writer]:
    """Get writer configuration by name"""
    return next((w for w in config.writers if w.name == writer_name), None)

def get_pipeline_config(config: Config, pipeline_name: str) -> Optional[Pipeline]:
    """Get pipeline configuration by name"""
    return next((p for p in config.pipelines if p.name == pipeline_name), None)
