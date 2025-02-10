import hypersync
from typing import List, Optional, cast, Dict
from dataclasses import dataclass
import json
from pathlib import Path

@dataclass
class EventConfig:
    name: str
    signature: str
    column_mapping: Dict[str, hypersync.DataType]

@dataclass
class Config:
    hypersync_url: str
    hypersync_api_key: str
    events: List[EventConfig]
    contract_identifier_signatures: List[str]
    from_block: int
    to_block: Optional[int]
    items_per_section: int
    parquet_output_path: str

def load_config(config_path: str) -> Config: 
    json_data_str = Path(config_path).read_text()
    config_data = json.loads(json_data_str)
    config = Config(**config_data)
    config.events = [EventConfig(**e) for e in config_data['events']]
    return config
 