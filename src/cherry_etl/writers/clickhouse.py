import logging
from typing import Dict, cast as type_cast
import pyarrow as pa
from ..writers.base import DataWriter
from ..config import ClickHouseWriterConfig
import asyncio

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
    elif pa.types.is_large_string(dt):
        return "String"
    elif pa.types.is_binary(dt):
        return "String"  # ClickHouse uses String for binary data too
    elif pa.types.is_large_binary(dt):
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
        dt = type_cast(pa.ListType, dt)
        return f"Array({pyarrow_type_to_clickhouse(dt.value_type)})"
    elif pa.types.is_large_list(dt):
        dt = type_cast(pa.LargeListType, dt)
        return f"Array({pyarrow_type_to_clickhouse(dt.value_type)})"
    elif pa.types.is_struct(dt):
        dt = type_cast(pa.StructType, dt)
        fields = [
            f"{field.name} {pyarrow_type_to_clickhouse(field.type)}"
            for field in list(dt)
        ]
        return f"Tuple({', '.join(fields)})"
    elif pa.types.is_map(dt):
        dt = type_cast(pa.MapType, dt)
        key_type = pyarrow_type_to_clickhouse(dt.key_type)
        item_type = pyarrow_type_to_clickhouse(dt.item_type)
        return f"Map({key_type}, {item_type})"
    elif pa.types.is_decimal128(dt):
        dt = type_cast(pa.Decimal128Type, dt)
        # For Decimal128, we can get precision and scale
        precision = dt.precision
        scale = dt.scale
        return f"Decimal({precision}, {scale})"
    elif pa.types.is_decimal256(dt):
        dt = type_cast(pa.Decimal256Type, dt)
        # For Decimal256, we can get precision and scale
        precision = dt.precision
        scale = dt.scale
        return f"Decimal({precision}, {scale})"
    else:
        raise Exception(f"Unimplemented pyarrow type: {dt}")


class Writer(DataWriter):
    def __init__(self, config: ClickHouseWriterConfig):
        self.client = config.client
        self.order_by = config.order_by
        self.codec = config.codec
        self.skip_index = config.skip_index
        self.first_insert = True
        self.anchor_table = config.anchor_table
        self.engine = config.engine

    async def _create_table_if_not_exists(self, table_name: str, schema: pa.Schema):
        if not await self._check_table_exists(table_name):
            await self._create_table(table_name, schema)
        else:
            logger.debug(f"table {table_name} already exists so skipping creation")

    async def _check_table_exists(self, table_name: str) -> bool:
        res = await self.client.query(
            f"SELECT count() > 0 as table_exists FROM system.tables WHERE database = '{self.client.client.database}' AND name = '{table_name}'"
        )

        return bool(res.result_rows[0][0])

    async def _create_table(self, table_name: str, schema: pa.Schema) -> None:
        columns = []

        for field in schema:
            ch_type = pyarrow_type_to_clickhouse(field.type)
            col_def = f"`{field.name}` {ch_type}"

            if table_name in self.codec:
                table_codec = self.codec[table_name]

                if field.name in table_codec:
                    col_def += f" CODEC({table_codec[field.name]})"

            columns.append(col_def)

        order_by = "tuple()"
        if table_name in self.order_by:
            col_list = self.order_by[table_name]
            if len(col_list) > 0:
                order_by = f"({', '.join(col_list)})"

        create_table_query = f"""
        CREATE TABLE {table_name} (
            {", ".join(columns)}
        ) ENGINE = {self.engine} 
        ORDER BY {order_by}
        """

        logger.debug(f"creating table with: {create_table_query}")

        await self.client.command(create_table_query)

        if table_name in self.skip_index:
            for idx in self.skip_index[table_name]:
                skip_index = f"ALTER TABLE {table_name} ADD INDEX {idx.name} {idx.val} TYPE {idx.type_} GRANULARITY {idx.granularity}"
                logger.debug(f"creating index with {skip_index}")
                await self.client.command(skip_index)

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
        # create tables if this is the first insert
        if self.first_insert:
            tasks = []

            for table_name, table_data in data.items():
                task = asyncio.create_task(
                    self._create_table_if_not_exists(table_name, table_data.schema),
                    name=f"create table {table_name}",
                )
                tasks.append(task)

            for task in tasks:
                await task

            self.first_insert = False

        # insert into all tables except the anchor table in parallel
        tasks = []
        for table_name, table_data in data.items():
            if table_name == self.anchor_table:
                continue

            task = asyncio.create_task(
                self.client.insert_arrow(table_name, table_data),
                name=f"write to {table_name}",
            )

            tasks.append(task)

        for task in tasks:
            await task

        # insert into anchor table after all other inserts are done
        if self.anchor_table is not None:
            table_data = data[self.anchor_table]
            await self.client.insert_arrow(self.anchor_table, table_data)
