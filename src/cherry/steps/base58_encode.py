from typing import Dict
from copy import deepcopy

from ..config import Base58EncodeConfig
from cherry_core import base58_encode
import pyarrow as pa
from .util import arrow_schema_binary_to_string


def execute(
    data: Dict[str, pa.Table], config: Base58EncodeConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    table_names = data.keys() if config.tables is None else config.tables

    for table_name in table_names:
        table = data[table_name]
        batches = table.to_batches()
        out_batches = []

        for batch in batches:
            out_batches.append(base58_encode(batch))

        new_schema = arrow_schema_binary_to_string(table.schema)
        data[table_name] = pa.Table.from_batches(out_batches, schema=new_schema)

    return data
