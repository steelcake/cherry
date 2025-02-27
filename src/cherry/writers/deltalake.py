import logging
from typing import Dict
import pyarrow as pa
from ..writers.base import DataWriter
from ..config import DeltaLakeWriterConfig
from deltalake import write_deltalake

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: DeltaLakeWriterConfig):
        self.base_path = config.base_path

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        for table_name, table_data in data.items():
            write_deltalake(self.base_path + "/" + table_name, data=table_data)

