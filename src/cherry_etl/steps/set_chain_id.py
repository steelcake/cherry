from typing import Dict

from ..config import SetChainIdConfig
import pyarrow as pa


def execute(data: Dict[str, pa.Table], config: SetChainIdConfig) -> Dict[str, pa.Table]:
    out = {}

    for table_name, table in data.items():
        o = table
        for name in table.schema.names:
            if name == "chain_id":
                o = table.drop_columns("chain_id")
                break
        out[table_name] = o.append_column(
            "chain_id",
            pa.repeat(
                pa.scalar(config.chain_id, type=pa.uint64()), size=table.num_rows
            ),
        )

    return out
