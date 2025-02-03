import asyncio, logging
from typing import Dict
from src.ingesters.base import Data
from src.writers.base import DataWriter


logger = logging.getLogger(__name__)

class Writer:
    """Handles writing data to multiple targets"""
    def __init__(self, writers: Dict[str, DataWriter]):
        self._writers = writers
        logger.info(f"Initialized Writer with {len(writers)} writers: {', '.join(writers.keys())}")


    async def write(self, data: Data) -> None:
        """Write data to all configured targets"""
        if not self._writers:
            logger.error("No writers initialized")
            return


        if not data or not data.events:
            logger.debug("No data to write")
            return

        try:
            # Create separate copies for each writer
            writer_data = {}
            for writer_type in self._writers.keys():

                writer_data[writer_type] = Data(
                    events={name: df.clone() for name, df in data.events.items()} if data.events else None,
                    blocks={name: df.clone() for name, df in data.blocks.items()} if data.blocks else None,
                    transactions=data.transactions
                )


            # Write in parallel to all targets
            write_tasks = {
                writer_type: asyncio.create_task(
                    writer.write(writer_data[writer_type]),
                    name=f"write_{writer_type}"
                )
                for writer_type, writer in self._writers.items()

            }
            
            if write_tasks:
                logger.info(f"Writing batch to {len(write_tasks)} targets")
                results = await asyncio.gather(*write_tasks.values(), return_exceptions=True)
                
                for writer_type, result in zip(write_tasks.keys(), results):
                    if isinstance(result, Exception):
                        logger.error(f"Error in {writer_type} writer: {result}")
                        raise result



        except Exception as e:
            logger.error(f"Error during parallel write: {e}")
            logger.error(f"Error occurred at line {e.__traceback__.tb_lineno}")
            raise 