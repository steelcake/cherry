from decimal import Decimal

import pyarrow as pa
import pytest
from deltalake import DeltaTable

from cherry.config import DeltaLakeWriterConfig
from cherry.writers.delta_lake import Writer


@pytest.fixture
def blocks():
    return pa.table(
        {
            "number": pa.array([1, 2, 3], type=pa.uint64()),
            "timestamp": pa.array(
                [
                    Decimal("1654321098765432109"),
                    Decimal("1754321098765432109"),
                    Decimal("1854321098765432109"),
                ],
                type=pa.decimal128(38, 0),
            ),
        },
        schema=pa.schema(
            [
                pa.field("number", pa.uint64(), True),
                pa.field("timestamp", pa.decimal128(38, 0), True),
            ]
        ),
    )


@pytest.mark.asyncio
async def test_delta_lake_writer(tmp_path, blocks):
    # Setup the writer with file path config
    config = DeltaLakeWriterConfig(table_uri=str(tmp_path))
    writer = Writer(config)

    # Write data using the writer
    table_name = "test_table"
    await writer.write_table(table_name, blocks)

    # Read back the data directly using Delta Lake API
    table_path = f"{tmp_path}/{table_name}"
    delta_table = DeltaTable(table_path)
    result = delta_table.to_pyarrow_table()

    # Verify the correct number of rows and columns
    assert result.num_rows == blocks.num_rows
    assert sorted(result.column_names) == sorted(blocks.column_names)
