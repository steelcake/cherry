from typing import Dict

from cherry_core import evm_validate_block_data
from ..config import EvmValidateBlockDataConfig
import pyarrow as pa
from .util import arrow_table_to_batch


def execute(
    data: Dict[str, pa.Table], config: EvmValidateBlockDataConfig
) -> Dict[str, pa.Table]:
    evm_validate_block_data(
        blocks=arrow_table_to_batch(data[config.blocks]),
        transactions=arrow_table_to_batch(data[config.transactions]),
        logs=arrow_table_to_batch(data[config.logs]),
        traces=arrow_table_to_batch(data[config.traces]),
    )

    return data
