from typing import Dict
from copy import deepcopy

from .. import utils
from ..config import GlaciersEventsConfig
import pyarrow as pa
import glaciers as gl
import polars as pl


def execute(
    data: Dict[str, pa.Table], config: GlaciersEventsConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)
    decoded_dict = {}

    input_table = data[config.input_table]
    input_df = pl.DataFrame(pl.from_arrow(input_table))

    decoded_df = gl.decode_df(config.decoder_type, input_df, config.abi_db_path)

    decoded_dict[config.output_table] = decoded_df

    output_table = utils.pl_data_to_pyarrow(decoded_dict)

    data.update(output_table)
    return data
