import logging
from typing import Dict
import pyarrow as pa
from ..writers.base import DataWriter
from ..config import ClickHouseWriterConfig

logger = logging.getLogger(__name__)


def pyarrow_type_to_clickhouse(dt: pa.DataType) -> str:
    if pa.types.is_boolean(dt):
        return "Bool"
    elif pa.types.is_int8(dt):
        return "Int8"
    elif pa.types.is_int16(dt):
        return "Int16"
    elif pa.types.is_int32(dt):
        return "Int32"
    elif pa.types.is_int64(dt):
        return "Int64"
    elif pa.types.is_uint8(dt):
        return "UInt8"
    elif pa.types.is_uint16(dt):
        return "UInt16"
    elif pa.types.is_uint32(dt):
        return "UInt32"
    elif pa.types.is_uint64(dt):
        return "UInt64"
    elif pa.types.is_float16(dt):
        return "Float32"  # ClickHouse doesn't support Float16
    elif pa.types.is_float32(dt):
        return "Float32"
    elif pa.types.is_float64(dt):
        return "Float64"
    elif pa.types.is_string(dt):
        return "String"
    elif pa.types.is_binary(dt):
        return "String"  # ClickHouse uses String for binary data too
    elif pa.types.is_date32(dt):
        return "Date"  # Date32 in Arrow is the same as Date in ClickHouse
    elif pa.types.is_date64(dt):
        return "DateTime"  # Date64 maps to DateTime
    elif pa.types.is_timestamp(dt):
        return "DateTime"  # Timestamp maps to DateTime
    elif pa.types.is_time32(dt):
        return "Int32"  # Time32 in Arrow maps to Int32 in ClickHouse
    elif pa.types.is_time64(dt):
        return "Int64"  # Time64 in Arrow maps to Int64 in ClickHouse
    elif pa.types.is_list(dt):
        return f"Array({pyarrow_type_to_clickhouse(dt.value_type)})"
    elif pa.types.is_struct(dt):
        fields = [
            f"{field.name} {pyarrow_type_to_clickhouse(field.type)}" for field in dt
        ]
        return f"Tuple({', '.join(fields)})"
    elif pa.types.is_map(dt):
        key_type = pyarrow_type_to_clickhouse(dt.key_type)
        value_type = pyarrow_type_to_clickhouse(dt.value_type)
        return f"Map({key_type}, {value_type})"
    elif pa.types.is_decimal128(dt):
        # For Decimal128, we can get precision and scale
        precision = dt.precision
        scale = dt.scale
        return f"Decimal({precision}, {scale})"
    elif pa.types.is_decimal256(dt):
        # For Decimal256, we can get precision and scale
        precision = dt.precision
        scale = dt.scale
        return f"Decimal({precision}, {scale})"
    else:
        raise Exception(f"Unimplemented pyarrow type: {dt}")


class Writer(DataWriter):
    def __init__(self, config: ClickHouseWriterConfig):
        self.client = config.client

    def _create_table_if_not_exists(
        self, table_name: str, table_data: pa.RecordBatch
    ) -> None:
        schema = table_data.schema
        columns = []

        for field in schema:
            ch_type = pyarrow_type_to_clickhouse(field.type)
            columns.append(f"`{field.name}` {ch_type}")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(columns)}
        ) ENGINE = MergeTree()
        ORDER BY tuple()
        """

        self.client.command(create_table_query)
        logger.info(f"Created table {table_name} if it didn't exist")

    async def push_data(self, data: Dict[str, pa.RecordBatch]) -> None:
        for table_name, table_data in data.items():
            self._create_table_if_not_exists(table_name, table_data)
            self.client.insert_arrow(
                f"{table_name}", pa.Table.from_batches([table_data])
            )
