import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
import logging
from .config import (
    Base58EncodeConfig,
    CastByTypeConfig,
    CastConfig,
    EvmDecodeEventsConfig,
    EvmValidateBlockDataConfig,
    HexEncodeConfig,
    Pipeline,
    Step,
    StepKind,
    Config,
)
from typing import Dict, List, Callable
from cherry_core.ingest import start_stream
from .config import StepFormat
import pyarrow as pa
from .writers.writer import create_writer
from . import steps as step_def
import polars

logger = logging.getLogger(__name__)

POLARS_STEP = Callable[
    [Dict[str, polars.DataFrame], Step], Awaitable[Dict[str, polars.DataFrame]]
]

PYARROW_STEP = Callable[[Dict[str, pa.Table], Step], Awaitable[Dict[str, pa.Table]]]


@dataclass
class StepInfo:
    runner: POLARS_STEP | PYARROW_STEP
    format: StepFormat


class Context:
    def __init__(self):
        self.steps = {}

    def add_step(
        self,
        kind: str,
        runner: POLARS_STEP | PYARROW_STEP,
        format: StepFormat = StepFormat.POLARS,
    ):
        self.steps[kind] = StepInfo(runner=runner, format=format)


async def run_pipelines(config: Config, context: Context):
    tasks = {
        name: asyncio.create_task(run_pipeline(pipeline, context, name))
        for name, pipeline in config.pipelines.items()
    }

    for name, task in tasks.items():
        logger.debug(f"running task with name: {name}")
        await task


async def process_steps(
    record_batches: Dict[str, pa.Table],
    steps: List[Step],
    context: Context,
) -> Dict[str, pa.Table]:
    logger.debug(f"Processing pipeline steps: {[step.kind for step in steps]}")
    logger.debug(f"Available custom step handlers: {list(context.steps.keys())}")

    res = record_batches

    for step in steps:
        logger.debug(f"running step kind: {step.kind} name: {step.name}")

        if step.kind == StepKind.EVM_VALIDATE_BLOCK_DATA:
            assert isinstance(step.config, EvmValidateBlockDataConfig)
            res = step_def.evm_validate_block_data.execute(res, step.config)
        elif step.kind == StepKind.EVM_DECODE_EVENTS:
            assert isinstance(step.config, EvmDecodeEventsConfig)
            res = step_def.evm_decode_events.execute(res, step.config)
        elif step.kind == StepKind.CAST:
            assert isinstance(step.config, CastConfig)
            res = step_def.cast.execute(res, step.config)
        elif step.kind == StepKind.HEX_ENCODE:
            assert isinstance(step.config, HexEncodeConfig)
            res = step_def.hex_encode.execute(res, step.config)
        elif step.kind == StepKind.BASE58_ENCODE:
            assert isinstance(step.config, Base58EncodeConfig)
            res = await asyncio.to_thread(
                step_def.base58_encode.execute, res, step.config
            )
        elif step.kind == StepKind.CAST_BY_TYPE:
            assert isinstance(step.config, CastByTypeConfig)
            res = step_def.cast_by_type.execute(res, step.config)
        elif step.kind in context.steps:
            step_info = context.steps[step.kind]

            if step_info.format == StepFormat.POLARS:
                res = pyarrow_data_to_polars(res)
                res = polars_data_to_pyarrow(await step_info.runner(res, step))
            elif step_info.format == StepFormat.PYARROW:
                res = await step_info.runner(res, step)
            else:
                raise Exception(f"Unknown step format: {step_info.format}")

        else:
            raise Exception(f"Unknown step kind: {step.kind}")

    return res


def pyarrow_data_to_polars(data: Dict[str, pa.Table]) -> Dict[str, polars.DataFrame]:
    new_data = {}

    for table_name, table_data in data.items():
        new_data[table_name] = polars.from_arrow(table_data)

    return new_data


def polars_data_to_pyarrow(data: Dict[str, polars.DataFrame]) -> Dict[str, pa.Table]:
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


async def run_pipeline(pipeline: Pipeline, context: Context, pipeline_name: str):
    logger.info(f"Running pipeline: {pipeline_name}")

    stream = start_stream(pipeline.provider.config)

    writer = create_writer(pipeline.writer)

    while True:
        data = await stream.next()
        if data is None:
            break

        tables = {}

        for table_name, table_batch in data.items():
            tables[table_name] = pa.Table.from_batches([table_batch])

        processed = await process_steps(tables, pipeline.steps, context)

        await writer.push_data(processed)
