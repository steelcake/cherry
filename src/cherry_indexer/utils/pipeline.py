import asyncio
import logging
from ..config.parser import parse_config, Pipeline, Provider, Step, StepKind
from ..utils.logging_setup import setup_logging
from typing import Dict, List
import copy
from dotenv import load_dotenv
from cherry_core.ingest import (
    start_stream,
    StreamConfig,
    Provider as CoreProvider,
    ProviderConfig as CoreProviderConfig,
    EvmQuery,
)
from cherry_core import evm_validate_block_data, evm_decode_events
import dacite
import pyarrow as pa
from ..writers.writer import create_writer

logger = logging.getLogger(__name__)

class Context:
    def __init__(self):
        self.steps = {}
        self.from_block = {}
    
    def add_step(self, kind: str, step: callable):
        self.steps[kind] = step
    
    def set_from_block(self, pipeline_name: str, block_number: int):
        self.from_block[pipeline_name] = block_number

async def run_pipelines(path: str, context: Context):
    config = parse_config(path)

    tasks = {
        name: asyncio.create_task(
            run_pipeline(pipeline, context, name)
        )
        for name, pipeline in config.pipelines.items()
    }
    
    for name, task in tasks.items():
        try:
            await task
        except Exception as e:
            logger.error(f"Failed to run pipeline {name}: {str(e)}")
            raise Exception(f"Error running pipeline {name}: {e}")
        
def provider_to_stream_config(provider: Provider, from_block: int) -> StreamConfig:
    
    if provider.config.query is not None:
        provider.config.query['from_block'] = from_block

    provider_config = CoreProviderConfig(
        url=provider.config.url,
        max_num_retries=provider.config.max_num_retries,
        retry_backoff_ms=provider.config.retry_backoff_ms,
        retry_base_ms=provider.config.retry_base_ms,
        retry_ceiling_ms=provider.config.retry_ceiling_ms,
        http_req_timeout_millis=provider.config.http_req_timeout_millis
    )

    core_provider = CoreProvider(
        kind=provider.kind,
        config=provider_config
    )

    return StreamConfig(
        format=provider.config.format,
        query=dacite.from_dict(data_class=EvmQuery, data=provider.config.query),
        provider=core_provider
    )

async def process_steps(res: Dict[str, pa.RecordBatch], steps: List[Step], context: Context) -> Dict[str, pa.RecordBatch]:
    logger.debug(f"Processing steps: {[step.kind for step in steps]}")
    logger.debug(f"Available custom steps: {list(context.steps.keys())}")
    
    # First process the built-in steps
    for step in steps:
        res = copy.deepcopy(res)
        if step.kind == StepKind.EVM_VALIDATE_BLOCK:
            evm_validate_block_data(blocks=res["blocks"], transactions=res["transactions"], logs=res["logs"], traces=res["traces"])
            pass
        elif step.kind == StepKind.EVM_DECODE_EVENTS:
            logger.debug(f"Processing decode events step: {step.config}")
            res[step.config['output_table']] = evm_decode_events(step.config['event_signature'], res[step.config['input_table']], step.config['allow_decode_fail'])
        elif context.steps[step.kind]:
                res = context.steps[step.kind](res, step.config)

    return res

async def run_pipeline(pipeline: Pipeline, context: Context, pipeline_name: str):
    """Run a pipeline"""
    logger.info(f"Running pipeline: {pipeline.name}")

    stream_config = provider_to_stream_config(pipeline.provider, context.from_block.get(pipeline_name, 0))
    logger.info(stream_config)

    stream = start_stream(stream_config)

    writer = create_writer(pipeline.writer)

    while True:
        res = await stream.next()
        if res is None:
            break
            
        logger.info(f"Raw data num rows: {[record_batch.num_rows for _, record_batch in res.items()]}")
        res = await process_steps(res, pipeline.steps, context)
        logger.info(f"Processed data num rows: {[record_batch.num_rows for _, record_batch in res.items()]}")
        await writer.push_data(res)
        

        if res is None:
            break