import logging
from typing import Dict, Optional, AsyncGenerator
from src.config.parser import Config, Pipeline
from src.types.data import Data
from src.processors.sqd import SQDProcessor

logger = logging.getLogger(__name__)

class Ingester:
    """Factory class for creating and managing ingesters"""
    
    def __init__(self, config: Config):
        self.config = config
        self._processors = {}
        self._current_block = 0
        logger.info(f"Initialized ingester with {len(config.pipelines)} pipelines")

    async def process_pipeline(self, pipeline: Pipeline) -> AsyncGenerator[Data, None]:
        """Process data from a pipeline"""
        try:
            # Get or create processor
            if pipeline.name not in self._processors:
                provider = self.config.providers.get(pipeline.provider.name)
                if not provider:
                    raise ValueError(f"Provider {pipeline.provider.name} not found")

                if provider.kind != "sqd":
                    raise ValueError(f"Unsupported provider kind: {provider.kind}")

                # Get event signature from decode step
                event_signature = None
                allow_decode_fail = False
                for step in pipeline.steps:
                    if step.kind == "evm_decode_events":
                        event_signature = step.config.event_signature
                        allow_decode_fail = step.config.allow_decode_fail
                        break

                # Add event signature to query if needed
                query = pipeline.provider.config.query
                if event_signature and 'logs' in query:
                    query['logs'][0]['event_signatures'] = [event_signature]

                self._processors[pipeline.name] = SQDProcessor(
                    url=provider.config.url,
                    query=query,
                    event_signature=event_signature,
                    allow_decode_fail=allow_decode_fail
                )

            # Process data
            async for data in self._processors[pipeline.name].process_data():
                logger.info(f"Data: {data}")
                                
                if not data.is_empty():
                    self._current_block = data.get_latest_block()
                    yield data

        except Exception as e:
            logger.error(f"Error processing pipeline {pipeline.name}: {e}", exc_info=True)
            raise

    async def process_all(self) -> AsyncGenerator[Data, None]:
        """Process all configured pipelines"""
        for pipeline_name, pipeline in self.config.pipelines.items():
            logger.info(f"Processing pipeline: {pipeline_name}")
            async for data in self.process_pipeline(pipeline):
                yield data

    @property
    def current_block(self) -> int:
        """Get current block number being processed"""
        return self._current_block