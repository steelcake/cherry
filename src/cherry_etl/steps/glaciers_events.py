from typing import Dict

from .. import utils
from ..config import GlaciersEventsConfig, CastByTypeConfig
import pyarrow as pa
import glaciers as gl
import polars as pl


def execute(
    data: Dict[str, pa.Table], config: GlaciersEventsConfig
) -> Dict[str, pa.Table]:
    decoded_dict = {}

    input_table = data[config.input_table]
    cast_by_type_config = CastByTypeConfig(
        from_type=pa.decimal256(76, 0),
        to_type=pa.float64(),
    )
    input_table = utils.cast_table_by_type(input_table, cast_by_type_config)
    input_df = pl.DataFrame(pl.from_arrow(input_table))

    decoded_df = gl.decode_df(config.decoder_type, input_df, config.abi_db_path)

    decoded_dict[config.output_table] = decoded_df

    output_table = utils.pl_data_to_pyarrow(decoded_dict)

    data.update(output_table)
    return data
