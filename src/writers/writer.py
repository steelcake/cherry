import logging
from typing import Dict, List, Optional
from src.config.parser import Config
from src.types.data import Data
from src.writers.base import DataWriter
from src.writers.local_parquet import ParquetWriter

logger = logging.getLogger(__name__)

class Writer:
    """Manages multiple data writers"""
    
    def __init__(self, writers: Dict[str, DataWriter]):
        self.writers = writers
        logger.info(f"Initialized {len(writers)} writers: {', '.join(writers.keys())}")

    @staticmethod
    def initialize_writers(config: Config) -> Dict[str, DataWriter]:
        """Initialize writers from config"""
        writers = {}
        
        for name, writer_config in config.writers.items():
            try:
                if writer_config.kind == "local_parquet":
                    writers[name] = ParquetWriter(writer_config.config)
                else:
                    logger.warning(f"Unsupported writer kind: {writer_config.kind}")
            except Exception as e:
                logger.error(f"Failed to initialize writer {name}: {e}")
                raise
                
        return writers

    async def write(self, data: Data) -> None:
        """Write data to all configured writers"""
        for name, writer in self.writers.items():
            try:
                await writer.write(data)
            except Exception as e:
                logger.error(f"Error writing to {name}: {e}", exc_info=True)
                raise