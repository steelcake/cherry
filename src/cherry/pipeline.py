import asyncio
import logging
from .config import Pipeline, Step, StepKind, Config
from typing import Dict, List, Callable
import copy
from cherry_core.ingest import start_stream
from cherry_core import evm_validate_block_data, evm_decode_events, cast, prefix_hex_encode, hex_encode
import pyarrow as pa
from .writers.writer import create_writer

logger = logging.getLogger(__name__)


class Context:
    def __init__(self):
        self.steps = {}

    def add_step(self, kind: str, step: Callable):
        self.steps[kind] = step


async def run_pipelines(config: Config, context: Context):
    tasks = {
        name: asyncio.create_task(run_pipeline(pipeline, context, name))
        for name, pipeline in config.pipelines.items()
    }

    for name, task in tasks.items():
        logger.debug(f"running task with name: {name}")
        await task


async def process_steps(
    record_batches: Dict[str, pa.RecordBatch],
    steps: List[Step],
    context: Context,
) -> Dict[str, pa.RecordBatch]:
    logger.debug(f"Processing pipeline steps: {[step.kind for step in steps]}")
    logger.debug(f"Available custom step handlers: {list(context.steps.keys())}")

    res = record_batches

    for step in steps:
        res = copy.deepcopy(res)

        if step.kind == StepKind.EVM_VALIDATE_BLOCK:
            logger.debug("Validating EVM block data...")
            evm_validate_block_data(
                blocks=res["blocks"],
                transactions=res["transactions"],
                logs=res["logs"],
                traces=res["traces"],
            )
        elif step.kind == StepKind.EVM_DECODE_EVENTS:
            logger.debug(f"Decoding EVM events with config: {step.config}")
            res[step.config["output_table"]] = evm_decode_events(
                step.config["event_signature"],
                res[step.config["input_table"]],
                step.config["allow_decode_fail"],
            )
        elif step.kind == StepKind.CAST:
            logger.debug(f"Executing cast step: {step.config}")
            res[step.config["output_table"]] = cast(
                step.config["mappings"],
                res[step.config["input_table"]], 
                step.config["allow_cast_fail"]
            )
        elif step.kind == StepKind.HEX_ENCODE:
            logger.debug(f"Executing hex encode step: {step.config}")
            for table_name in step.config["tables"]:
                if not res.get("prefixed", True):
                    res[table_name] = hex_encode(
                        res[table_name]
                    )
                else:
                    res[table_name] = prefix_hex_encode(
                        res[table_name]
                    )

        elif step.kind in context.steps:
            logger.info(f"Executing custom step: {step.kind} {res}")
            res = context.steps[step.kind](res, step)
        else:
            logger.warning(f"Unknown step kind: {step.kind}")

        logger.debug(f"Step {step.kind} complete. Current tables: {list(res.keys())}")

    return res


async def run_pipeline(pipeline: Pipeline, context: Context, pipeline_name: str):
    logger.info(f"Running pipeline: {pipeline_name}")

    stream = start_stream(pipeline.provider.config)

    writer = create_writer(pipeline.writer)

    while True:
        batch = await stream.next()
        if batch is None:
            break

        logger.debug(
            f"Raw data num rows: {[(table_name, record_batch.num_rows) for table_name, record_batch in batch.items()]}"
        )

        processed = await process_steps(batch, pipeline.steps, context)

        logger.debug(
            f"Processed data num rows: {[(table_name, record_batch.num_rows) for table_name, record_batch in processed.items()]}"
        )

        await writer.push_data(processed)
