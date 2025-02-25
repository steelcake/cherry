import logging
from typing import Dict
import pyarrow as pa
from ..writers.base import DataWriter
from ..config.parser import ClickHouseWriterConfig 

logger = logging.getLogger(__name__)

class Writer(DataWriter):
    def __init__(self, config: ClickHouseWriterConfig):
        self.client = config.client

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        for table_name, table_data in data.items():
            self.client.insert_arrow(table_name, pa.Table.from_batches([table_data]))
 
