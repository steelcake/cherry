import pyarrow as pa
import pytest
from pathlib import Path
import pyarrow.parquet as pq

from cherry.config import LocalParquetWriterConfig
from cherry.writers.local_parquet import Writer


@pytest.fixture
def blocks():
    return pa.table(
        {
            "number": pa.array([1, 2, 3], type=pa.uint64()),
            "timestamp": pa.array(
                [
                    1654321098765432109,
                    1754321098765432109,
                    1854321098765432109,
                ],
                type=pa.int64(),
            ),
        },
        schema=pa.schema(
            [
                pa.field("number", pa.uint64(), True),
                pa.field("timestamp", pa.int64(), True),
            ]
        ),
    )


@pytest.mark.asyncio
async def test_local_parquet_writer(tmp_path, blocks):
    # Setup the writer with file path config
    config = LocalParquetWriterConfig(output_dir=str(tmp_path))
    writer = Writer(config)

    # Write data using the writer
    table_name = "test_table"
    await writer.push_data({table_name: blocks})

    # Read back the data directly using Parquet API
    output_dir = tmp_path / table_name
    parquet_files = list(output_dir.glob("*.parquet"))
    
    # Verify that at least one parquet file was created
    assert len(parquet_files) > 0
    
    # Read the first parquet file
    result = pq.read_table(parquet_files[0])

    # Verify the correct number of rows and columns
    assert result.num_rows == blocks.num_rows
    assert sorted(result.column_names) == sorted(blocks.column_names)
