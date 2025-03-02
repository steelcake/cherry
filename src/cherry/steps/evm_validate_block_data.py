from typing import Dict

from pyarrow import RecordBatch
from cherry_core import evm_validate_block_data
from ..config import EvmValidateBlockDataConfig


def execute(
    data: Dict[str, RecordBatch], config: EvmValidateBlockDataConfig
) -> Dict[str, RecordBatch]:
    evm_validate_block_data(
        blocks=data[config.blocks],
        transactions=data[config.transactions],
        logs=data[config.logs],
        traces=[config.traces],
    )

    return data
