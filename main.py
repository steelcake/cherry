import asyncio
import logging
from pathlib import Path
from src.config.parser import parse_config, Config, Pipeline, Provider
from src.utils.logging_setup import setup_logging
from typing import Dict
import sys
from dotenv import load_dotenv
import copy
from cherry_core.ingest import start_stream, StreamConfig, Provider as CoreProvider, ProviderConfig as CoreProviderConfig, EvmQuery, LogRequest
import dacite

load_dotenv()

# Set up logging
setup_logging()
logger = logging.getLogger(__name__)

def provider_to_stream_config(provider: Provider) -> StreamConfig:

    #logger.info(LogRequest(**provider.config.query['logs'][0]))

    return StreamConfig(
        format=provider.config.format,
        query=dacite.from_dict(data_class=EvmQuery, data=provider.config.query),
        provider=CoreProvider(kind=provider.kind, config=CoreProviderConfig(url=provider.config.url, max_num_retries=provider.config.max_num_retries, 
                                                                            retry_backoff_ms=provider.config.retry_backoff_ms, retry_base_ms=provider.config.retry_base_ms,
                                                                            retry_ceiling_ms=provider.config.retry_ceiling_ms, 
                                                                            http_req_timeout_millis=provider.config.http_req_timeout_millis)),
    )

async def run_pipeline(pipeline: Pipeline):
    """Run a pipeline"""
    logger.info(f"Running pipeline: {pipeline.name}")
    logger.info(pipeline.provider.config.query)

    stream_config = provider_to_stream_config(pipeline.provider)
    logger.info(stream_config)

    stream = start_stream(stream_config)

    while True:
        res = await stream.next()

        if res is None:
            break

        logger.info(res)

async def main():
    """Main entry point"""
    try:
        logger.info("Starting blockchain data ingestion")

        # Load configuration
        config = parse_config("config.yaml")

        for pipeline in config.pipelines.values():
            await run_pipeline(pipeline)

        sys.exit()

        writer = Writer(Writer.initialize_writers(config))

        # Process data
        await process_data(ingester, writer)

        logger.info("Blockchain data ingestion completed successfully")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(f"Error occurred at line {sys.exc_info()[2].tb_lineno}")
        raise
    finally:
        # Cleanup if needed
        pass

if __name__ == "__main__":
    asyncio.run(main())