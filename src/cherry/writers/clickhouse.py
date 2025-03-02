import logging
from typing import Dict
import pyarrow as pa
from ..writers.base import DataWriter
from ..config import ClickHouseWriterConfig

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: ClickHouseWriterConfig):
        self.client = config.client

    def _create_table_if_not_exists(
        self, table_name: str, table_data: pa.RecordBatch
    ) -> None:
        schema = table_data.schema
        columns = []

        # Map PyArrow types to ClickHouse types
        type_mapping = {
            pa.string(): "String",
            pa.int64(): "Int64",
            pa.int32(): "Int32",
            pa.float64(): "Float64",
            pa.float32(): "Float32",
            pa.bool_(): "UInt8",
            pa.timestamp("ns"): "DateTime64(9)",
            pa.binary(): "String",
        }

        for field in schema:
            ch_type = type_mapping.get(field.type, "String")
            columns.append(f"`{field.name}` {ch_type}")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(columns)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """

        try:
            self.client.command(create_table_query)
            logger.info(f"Created table {table_name} if it didn't exist")
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        for table_name, table_data in data.items():
            self._create_table_if_not_exists(table_name, table_data)
            self.client.insert_arrow(
                f"{table_name}", pa.Table.from_batches([table_data])
            )
