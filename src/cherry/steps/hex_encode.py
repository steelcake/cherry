from typing import Dict
from copy import deepcopy

from ..config import HexEncodeConfig
from cherry_core import hex_encode, prefix_hex_encode
import pyarrow as pa


def execute(data: Dict[str, pa.Table], config: HexEncodeConfig) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    decode_fn = prefix_hex_encode if config.prefixed else hex_encode

    table_names = data.keys() if config.tables is None else config.tables

    for table_name in table_names:
        batches = data[table_name].to_batches()
        out_batches = []

        for batch in batches:
            out_batches.append(decode_fn(batch))

        data[table_name] = pa.Table.from_batches(out_batches)

    return data
