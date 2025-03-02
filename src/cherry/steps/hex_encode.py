from typing import Dict
from copy import deepcopy

from pyarrow import RecordBatch
from ..config import HexEncodeConfig
from cherry_core import hex_encode, prefix_hex_encode


def execute(
    data: Dict[str, RecordBatch], config: HexEncodeConfig
) -> Dict[str, RecordBatch]:
    data = deepcopy(data)

    decode_fn = prefix_hex_encode if config.prefixed else hex_encode

    if config.tables is None:
        for table_name, table_data in data.items():
            data[table_name] = decode_fn(table_data)
    else:
        for table_name in config.tables:
            data[table_name] = decode_fn(data[table_name])

    return data
