import asyncio, logging
from typing import Dict
from src.ingesters.base import Data
from src.loaders.base import DataLoader

logger = logging.getLogger(__name__)

class Loader:
    """Handles writing data to multiple targets"""
    def __init__(self, loaders: Dict[str, DataLoader]):
        self._loaders = loaders
        logger.info(f"Initialized Loader with {len(loaders)} loaders: {', '.join(loaders.keys())}")

    async def load(self, data: Data) -> None:
        """Write data to all configured targets"""
        if not self._loaders:
            logger.error("No loaders initialized")
            return

        if not data or not data.events:
            logger.debug("No data to write")
            return

        try:
            # Create separate copies for each loader
            loader_data = {}
            for loader_type in self._loaders.keys():
                loader_data[loader_type] = Data(
                    events={name: df.clone() for name, df in data.events.items()} if data.events else None,
                    blocks={name: df.clone() for name, df in data.blocks.items()} if data.blocks else None,
                    transactions=data.transactions
                )

            # Write in parallel to all targets
            write_tasks = {
                loader_type: asyncio.create_task(
                    loader.load(loader_data[loader_type]),
                    name=f"write_{loader_type}"
                )
                for loader_type, loader in self._loaders.items()
            }
            
            if write_tasks:
                logger.info(f"Writing batch to {len(write_tasks)} targets")
                results = await asyncio.gather(*write_tasks.values(), return_exceptions=True)
                
                for loader_type, result in zip(write_tasks.keys(), results):
                    if isinstance(result, Exception):
                        logger.error(f"Error in {loader_type} writer: {result}")
                        raise result

        except Exception as e:
            logger.error(f"Error during parallel write: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 