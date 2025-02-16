import asyncio
import logging
from src.config.parser import parse_config, Pipeline, Provider, Step, Writer, WriterKind, StepConfig, Config, StepKind
from src.utils.logging_setup import setup_logging
from typing import Dict, List
import sys
from dotenv import load_dotenv
from cherry_core.ingest import (
    start_stream,
    StreamConfig,
    Provider as CoreProvider,
    ProviderConfig as CoreProviderConfig,
    EvmQuery,
)
from cherry_core import evm_validate_block_data, evm_decode_events, evm_signature_to_topic0
import dacite
import pyarrow as pa
from src.writers.writer import create_writer
from src.utils.pipeline import Context, run_pipelines

load_dotenv()

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

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