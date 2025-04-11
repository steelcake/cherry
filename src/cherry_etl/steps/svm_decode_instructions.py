from typing import Dict
from copy import deepcopy

from cherry_core import svm_decode_instructions, instruction_signature_to_arrow_schema
from ..config import SvmDecodeInstructionsConfig
import pyarrow as pa


def execute(
    data: Dict[str, pa.Table], config: SvmDecodeInstructionsConfig
) -> Dict[str, pa.Table]:
    data = deepcopy(data)

    input_table = data[config.input_table]
    input_batches = input_table.to_batches()

    output_batches = []

    for batch in input_batches:
        output_batches.append(
            svm_decode_instructions(
                config.instruction_signature, batch, config.allow_decode_fail
            )
        )

    output_table = pa.Table.from_batches(
        output_batches,
        schema=instruction_signature_to_arrow_schema(config.instruction_signature),
    )

    if config.hstack:
        for i, col in enumerate(input_table.columns):
            output_table = output_table.append_column(input_table.field(i), col)

    data[config.output_table] = output_table

    return data
