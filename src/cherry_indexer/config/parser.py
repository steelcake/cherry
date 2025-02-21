from dataclasses import dataclass
from typing import List, Dict, Optional
from enum import Enum
import yaml, logging
import copy, dacite
from cherry_core.ingest import ProviderKind, Format

logger = logging.getLogger(__name__)

class WriterKind(str, Enum):
    LOCAL_PARQUET = "local_parquet"
    AWS_WRANGLER_S3 = "aws_wrangler_s3"
    POSTGRES = "postgres"
    CLICKHOUSE = "clickhouse"

class StepKind(str, Enum):
    EVM_VALIDATE_BLOCK = 'evm_validate_block_data'
    EVM_DECODE_EVENTS = 'evm_decode_events'

class StepPhase(str, Enum):
    PRE_STREAM = 'pre_stream'
    STREAM = 'stream'
    POST_STREAM = 'post_stream'

@dataclass
class ProviderConfig:
    """Provider-specific configuration"""
    kind: Optional[ProviderKind] = None
    format: Optional[Format] = None
    url: Optional[str] = None
    max_num_retries: Optional[int] = None
    retry_backoff_ms: Optional[int] = None
    retry_base_ms: Optional[int] = None
    retry_ceiling_ms: Optional[int] = None
    http_req_timeout_millis: Optional[int] = None
    query: Optional[Dict] = None

@dataclass
class Provider:
    """Data provider configuration"""
    name: Optional[str] = None
    config: Optional[ProviderConfig] = None

@dataclass
class WriterConfig:
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

@dataclass
class Writer:
    """Data writer configuration"""
    name: Optional[str] = None
    kind: Optional[WriterKind] = None
    config: Optional[WriterConfig] = None

@dataclass
class Step:
    """Pipeline step configuration"""
    name: str
    kind: Optional[str] = None
    config: Optional[Dict] = None
    phase: Optional[StepPhase] = StepPhase.STREAM

@dataclass
class Pipeline:
    """Data pipeline configuration"""
    provider: Provider
    writer: Writer
    steps: List[Step]
    name: Optional[str] = None

@dataclass
class Config:
    """Main configuration"""
    project_name: str
    description: str
    providers: Dict[str, Provider]
    writers: Dict[str, Writer]
    pipelines: Dict[str, Pipeline]
def prepare_config(config: Dict) -> Dict:
    """Prepare configuration for use"""

    config = copy.deepcopy(config)

    pipelines = {}

    for pipeline_name, pipeline in config['pipelines'].items():
        pipelines[pipeline_name] = copy.deepcopy(pipeline)

        provider =  config['providers'][pipeline['provider']['name']]

        if provider is not None:

            provider_config = copy.deepcopy(provider['config'])
            # Overwrite with pipeline provider config
            provider_config.update(copy.deepcopy(pipeline['provider']['config']))
            pipelines[pipeline_name]['provider']['config'] = provider_config
            
            # Use pipeline provider kind if specified, otherwise use provider kind
            pipelines[pipeline_name]['provider']['config']['kind'] = copy.deepcopy(pipeline['provider']['config'].get('kind', provider['config']['kind']))

        
        writer = config['writers'][pipeline['writer']['name']]

        if writer is not None:

            # Start with writer config as base
            writer_config = copy.deepcopy(writer['config'])
            # Overwrite with pipeline writer config
            writer_config.update(copy.deepcopy(pipeline['writer']['config']))
            pipelines[pipeline_name]['writer']['config'] = writer_config
            
            # Use pipeline writer kind if specified, otherwise use writer kind
            pipelines[pipeline_name]['writer']['kind'] = copy.deepcopy(pipeline['writer'].get('kind', writer['kind']))

    config['pipelines'] = pipelines

    return config


def parse_config(config_path: str) -> Config:
    """Parse configuration from YAML file"""

    try:
        with open(config_path, 'r') as f:
            raw_config = yaml.safe_load(f)
            
            prepared_config = prepare_config(raw_config)
            
            # Parse configuration
            config = dacite.from_dict(data_class=Config, data=prepared_config, config=dacite.Config(cast=[Enum]))
            
            logger.info(f"Parsed Config: {config}")

            return config
            
    except Exception as e:
        logger.error(f"Error parsing config file {config_path}: {e}")
        raise

