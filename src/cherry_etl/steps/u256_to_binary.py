from typing import Dict
from copy import deepcopy

from cherry_core import u256_to_binary
import pyarrow as pa

from cherry_etl.config import U256ToBinaryConfig
from .util import arrow_schema_cast_by_type


def execute(
    data: Dict[str, pa.Table], config: U256ToBinaryConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    table_names = data.keys() if config.tables is None else config.tables

    for table_name in table_names:
        table = data[table_name]
        batches = table.to_batches()
        out_batches = []

        for batch in batches:
            out_batches.append(u256_to_binary(batch))

        new_schema = arrow_schema_cast_by_type(
            table.schema, pa.decimal256(76, 0), pa.binary()
        )
        data[table_name] = pa.Table.from_batches(out_batches, schema=new_schema)

    return data
