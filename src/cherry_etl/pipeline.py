import asyncio
import logging
from .config import (
    Base58EncodeConfig,
    CastByTypeConfig,
    CastConfig,
    CustomStepConfig,
    EvmDecodeEventsConfig,
    EvmValidateBlockDataConfig,
    HexEncodeConfig,
    Pipeline,
    Step,
    StepKind,
)
from typing import Dict, List
from cherry_core.ingest import start_stream
import pyarrow as pa
from .writers.writer import create_writer
from . import steps as step_def
import polars as pl
from copy import deepcopy

logger = logging.getLogger(__name__)


async def process_steps(
    data: Dict[str, pa.Table],
    steps: List[Step],
) -> Dict[str, pa.Table]:
    logger.debug(
        f"Processing pipeline steps: {[[step.kind, step.name] for step in steps]}"
    )

    data = deepcopy(data)

    for step in steps:
        logger.debug(f"running step kind: {step.kind} name: {step.name}")

        if step.kind == StepKind.EVM_VALIDATE_BLOCK_DATA:
            assert isinstance(step.config, EvmValidateBlockDataConfig)
            data = step_def.evm_validate_block_data.execute(data, step.config)
        elif step.kind == StepKind.EVM_DECODE_EVENTS:
            assert isinstance(step.config, EvmDecodeEventsConfig)
            data = step_def.evm_decode_events.execute(data, step.config)
        elif step.kind == StepKind.CAST:
            assert isinstance(step.config, CastConfig)
            data = step_def.cast.execute(data, step.config)
        elif step.kind == StepKind.HEX_ENCODE:
            assert isinstance(step.config, HexEncodeConfig)
            data = step_def.hex_encode.execute(data, step.config)
        elif step.kind == StepKind.BASE58_ENCODE:
            assert isinstance(step.config, Base58EncodeConfig)
            data = await asyncio.to_thread(
                step_def.base58_encode.execute, data, step.config
            )
        elif step.kind == StepKind.CAST_BY_TYPE:
            assert isinstance(step.config, CastByTypeConfig)
            data = step_def.cast_by_type.execute(data, step.config)
        elif step.kind == StepKind.CUSTOM:
            assert isinstance(step.config, CustomStepConfig)
            pl_data = pyarrow_data_to_pl(data)
            pl_data = await asyncio.to_thread(
                step.config.runner, pl_data, step.config.context
            )
            data = pl_data_to_pyarrow(pl_data)
        else:
            raise Exception(f"Unknown step kind: {step.kind}")

    return data


def pyarrow_data_to_pl(data: Dict[str, pa.Table]) -> Dict[str, pl.DataFrame]:
    new_data = {}

    for table_name, table_data in data.items():
        new_data[table_name] = pl.from_arrow(table_data)

    return new_data


def pl_data_to_pyarrow(data: Dict[str, pl.DataFrame]) -> Dict[str, pa.Table]:
    new_data = {}

    for table_name, table_data in data.items():
        new_data[table_name] = pyarrow_large_binary_to_binary(table_data.to_arrow())

    return new_data


def pyarrow_large_binary_to_binary(table: pa.Table) -> pa.Table:
    columns = []
    for column in table.columns:
        if column.type == pa.large_binary():
            columns.append(column.cast(pa.binary(), safe=True))
        elif column.type == pa.large_string():
            columns.append(column.cast(pa.string(), safe=True))
        else:
            columns.append(column)

    return pa.Table.from_arrays(columns, names=table.column_names)


async def run_pipeline(pipeline_name: str, pipeline: Pipeline):
    logger.info(f"Running pipeline: {pipeline_name}")

    stream = start_stream(pipeline.provider)

    writer = create_writer(pipeline.writer)

    while True:
        data = await stream.next()
        if data is None:
            break

        tables = {}

        for table_name, table_batch in data.items():
            tables[table_name] = pa.Table.from_batches([table_batch])

        processed = await process_steps(tables, pipeline.steps)

        await writer.push_data(processed)
