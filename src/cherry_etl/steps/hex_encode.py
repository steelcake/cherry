from typing import Dict
from copy import deepcopy

from ..config import HexEncodeConfig
from cherry_core import hex_encode, prefix_hex_encode
import pyarrow as pa
from .util import arrow_schema_binary_to_string


def execute(data: Dict[str, pa.Table], config: HexEncodeConfig) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    decode_fn = prefix_hex_encode if config.prefixed else hex_encode

    table_names = data.keys() if config.tables is None else config.tables

    for table_name in table_names:
        table = data[table_name]
        batches = table.to_batches()
        out_batches = []

        for batch in batches:
            out_batches.append(decode_fn(batch))

        new_schema = arrow_schema_binary_to_string(table.schema)
        data[table_name] = pa.Table.from_batches(out_batches, schema=new_schema)

    return data
