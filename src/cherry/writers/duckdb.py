import logging
from typing import Dict, cast as type_cast
import pyarrow as pa
from ..writers.base import DataWriter
from ..config import DuckDBWriterConfig
import asyncio

logger = logging.getLogger(__name__)


class Writer(DataWriter):
    def __init__(self, config: DuckDBWriterConfig):
        self.connection = config.connection
        self.anchor_table = config.anchor_table
        self.first_insert = True

    async def _create_table_if_not_exists(self, table_name: str, schema: pa.Schema):
        if not await self._check_table_exists(table_name):
            await self._create_table(table_name, schema)
        else:
            logger.debug(f"table {table_name} already exists so skipping creation")

    async def _check_table_exists(self, table_name: str) -> bool:
        query = f"SELECT count(*) > 0 as table_exists FROM information_schema.tables WHERE table_name = '{table_name}'"
        result = self.connection.execute(query).fetchone()
        return bool(result[0])

    async def _create_table(self, table_name: str, schema: pa.Schema) -> None:
        columns = []
        for field in schema:
            duckdb_type = self._pyarrow_type_to_duckdb(field.type)
            columns.append(f'"{field.name}" {duckdb_type}')

        create_table_query = f"""
        CREATE TABLE {table_name} (
            {", ".join(columns)}
        )
        """

        logger.debug(f"creating table with: {create_table_query}")
        self.connection.execute(create_table_query)

    def _pyarrow_type_to_duckdb(self, dt: pa.DataType) -> str:
        type_mapping = {
            'boolean': 'BOOLEAN',
            'int8': 'TINYINT',
            'int16': 'SMALLINT',
            'int32': 'INTEGER',
            'int64': 'BIGINT',
            'uint8': 'UTINYINT',
            'uint16': 'USMALLINT',
            'uint32': 'UINTEGER',
            'uint64': 'UBIGINT',
            'float16': 'FLOAT',
            'float32': 'FLOAT',
            'float64': 'DOUBLE',
            'string': 'VARCHAR',
            'binary': 'BLOB',
            'date32': 'DATE',
            'date64': 'DATE',
            'timestamp': 'TIMESTAMP',
            'time32': 'TIME',
            'time64': 'TIME'
        }

        for pyarrow_type, duckdb_type in type_mapping.items():
            if getattr(pa.types, f'is_{pyarrow_type}')(dt):
                return duckdb_type

        if pa.types.is_list(dt):
            dt = type_cast(pa.ListType, dt)
            return f"ARRAY({self._pyarrow_type_to_duckdb(dt.value_type)})"
        elif pa.types.is_struct(dt):
            dt = type_cast(pa.StructType, dt)
            fields = [
                f"{field.name} {self._pyarrow_type_to_duckdb(field.type)}"
                for field in list(dt)
            ]
            return f"STRUCT({', '.join(fields)})"
        elif pa.types.is_map(dt):
            dt = type_cast(pa.MapType, dt)
            key_type = self._pyarrow_type_to_duckdb(dt.key_type)
            value_type = self._pyarrow_type_to_duckdb(dt.item_type)
            return f"MAP({key_type}, {value_type})"
        elif pa.types.is_decimal128(dt):
            dt = type_cast(pa.Decimal128Type, dt)
            precision = min(dt.precision, 38)
            return f"DECIMAL({precision}, {dt.scale})"
        elif pa.types.is_decimal256(dt):
            dt = type_cast(pa.Decimal256Type, dt)
            precision = min(dt.precision, 38)
            return f"DECIMAL({precision}, {dt.scale})"
        else:
            raise Exception(f"Unimplemented pyarrow type: {dt}")

    async def push_data(self, data: Dict[str, pa.Table]) -> None:
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

        tasks = []
        for table_name, table_data in data.items():
            if table_name == self.anchor_table:
                continue

            task = asyncio.create_task(
                self._insert_table(table_name, table_data),
                name=f"write to {table_name}",
            )
            tasks.append(task)

        for task in tasks:
            await task

        if self.anchor_table and self.anchor_table in data:
            await self._insert_table(self.anchor_table, data[self.anchor_table])

    async def _insert_table(self, table_name: str, table: pa.Table) -> None:
        try:
            converted_table = table
            for i, field in enumerate(table.schema):
                if pa.types.is_decimal256(field.type):
                    # Create new field with Decimal128 type
                    new_field = pa.field(
                        field.name, 
                        pa.decimal128(min(field.type.precision, 38), field.type.scale)
                    )
                    # Cast the column to Decimal128
                    converted_table = converted_table.set_column(
                        i, 
                        new_field, 
                        converted_table.column(i).cast(new_field.type)
                    )
            
            self.connection.register(table_name + "_temp", converted_table)
            
            self.connection.execute(f"DELETE FROM {table_name}")
            
            self.connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_temp")
            
            self.connection.unregister(table_name + "_temp")
            
            logger.debug(f"Successfully wrote to table {table_name}")
        except Exception as e:
            logger.error(f"Error writing to table {table_name}: {str(e)}")
            raise 