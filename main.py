import asyncio
import logging
from pathlib import Path
from src.config.parser import parse_config, Pipeline, Provider, Step, Writer, WriterKind, StepConfig, Config, StepKind
from src.utils.logging_setup import setup_logging
from typing import Dict, List
import sys
from dotenv import load_dotenv
import copy
from cherry_core.ingest import (
    start_stream,
    StreamConfig,
    Provider as CoreProvider,
    ProviderConfig as CoreProviderConfig,
    EvmQuery,
)
from cherry_core import evm_validate_block_data, evm_decode_events, evm_signature_to_topic0
import dacite, os, datetime
import pyarrow as pa
import pyarrow.parquet as pq
from src.writers.writer import create_writer
from src.writers.local_parquet import ParquetWriter

load_dotenv()

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

class Context:
    def __init__(self):
        self.steps = {}
    
    def add_steps(self, kind: str, step: callable):
        self.steps[kind] = step

async def run_pipelines(path: str, context: Context):
    config = parse_config(path)

    tasks = {
        name: asyncio.create_task(
            run_pipeline(pipeline, context)
        )
        for name, pipeline in config.pipelines.items()
    }
    
    for name, task in tasks.items():
        try:
            await task
        except Exception as e:
            logger.error(f"Failed to run pipeline {name}: {str(e)}")
            raise Exception(f"Error running pipeline {name}: {e}")
        
def provider_to_stream_config(provider: Provider) -> StreamConfig:

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

    for step in steps:
            if step.kind ==  StepKind.EVM_VALIDATE_BLOCK:
                evm_validate_block_data(blocks=res["blocks"], transactions=res["transactions"], logs=res["logs"], traces=res["traces"])
            elif step.kind == StepKind.EVM_VALIDATE_BLOCK:
                res[step.config.output_table] = evm_decode_events(step.config.event_signature, res[step.config.input_table], step.config.allow_decode_fail)
            elif context.steps[step.kind]:
                res = context.steps[step.kind](res, step.config)

    return res

async def combine_data(res: Dict[str, pa.RecordBatch], combined_data: Dict[str, pa.RecordBatch]) -> Dict[str, pa.RecordBatch]:
    for table_name, data in res.items():
        if table_name not in combined_data:
            combined_data[table_name] = data
        else:
            combined_data[table_name] = pa.concat_batches([combined_data[table_name], data])
    return combined_data

async def run_pipeline(pipeline: Pipeline, context: Context):
    """Run a pipeline"""
    logger.info(f"Running pipeline: {pipeline.name}")
    combined_data = {}

    stream_config = provider_to_stream_config(pipeline.provider)
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
        combined_data = await combine_data(res, combined_data)
        logger.info(f"Combined data num rows: {[record_batch.num_rows for _, record_batch in combined_data.items()]}")
        await writer.push_data(combined_data)
        

        if res is None:
            break

        #sys.exit()

async def main():
    """Main entry point for the application"""
    try:
        logger.info("Starting blockchain data ingestion")

        await run_pipelines(path="config.yaml", context=Context())

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Error occurred at line {sys.exc_info()[2].tb_lineno}")
        raise



if __name__ == "__main__":
    asyncio.run(main())