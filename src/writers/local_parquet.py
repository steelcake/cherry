import logging
from pathlib import Path
from typing import Dict, Optional
import pyarrow as pa
import pyarrow.parquet as pq
from src.writers.base import DataWriter
from datetime import datetime
from src.types.data import Data

logger = logging.getLogger(__name__)

class ParquetWriter(DataWriter):
    """Generic writer for Arrow RecordBatches to local Parquet files"""
    
    def __init__(self, config: Dict):
        super().__init__()
        # Get path from config, use 'data' if empty or not provided
        config_path = getattr(config, 'path', 'data')
        self.path = Path(config_path if config_path else 'data').resolve()
        
        # Create base data directory
        self.path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized ParquetWriter at {self.path}")

    async def push_data(self, data: Data):
        """Push data to parquet files"""
        try:
            # Process each collection
            for collection_name, collection in data.items():
                logger.debug(f"Writing collection: {collection_name}")
                if isinstance(collection, dict):
                    # Handle nested dictionary structure
                    for inner_name, batch in collection.items():
                        if batch and hasattr(batch, 'num_rows') and batch.num_rows > 0:
                            # Create file path
                            file_path = self._get_file_path(
                                collection_name,
                                inner_name,
                            )
                            
                            # Ensure directory exists
                            file_path.parent.mkdir(parents=True, exist_ok=True)
                            
                            # Write batch to parquet
                            logger.info(f"Writing {batch.num_rows} rows to {file_path}")
                            self._write_batch(batch, file_path)
                elif hasattr(collection, 'num_rows') and collection.num_rows > 0:
                    # Handle direct RecordBatch
                    # Create file path
                    file_path = self._get_file_path(
                        collection_name,
                        None,
                    )
                    
                    # Ensure directory exists
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Write batch to parquet
                    logger.info(f"Writing {collection.num_rows} rows to {file_path}")
                    self._write_batch(collection, file_path)
        except Exception as e:
            logger.error(f"Error writing parquet files: {e}", exc_info=True)
            raise

    def _get_file_path(self, collection_name: str, inner_name: Optional[str]) -> Path:
        """Generate file path for a collection"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if inner_name and inner_name != collection_name:
            return (self.path / collection_name / inner_name / f"{timestamp}.parquet").resolve()
        return (self.path / collection_name / f"{timestamp}.parquet").resolve()

    def _write_batch(self, batch, path: Path):
        """Write a RecordBatch to parquet"""
        try:
            table = pa.Table.from_batches([batch])
            pq.write_table(table, str(path))
            logger.info(f"Successfully wrote {batch.num_rows} rows to {path}")
        except Exception as e:
            logger.error(f"Error writing batch to {path}: {e}", exc_info=True)
            raise

    async def write(self, data) -> None:
        """Write data from Data format"""
        # Convert Data object to RecordBatches dictionary
        await self.push_data(data)