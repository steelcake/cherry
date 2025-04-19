import pyarrow as pa
import chdb
import pytest
from contextlib import closing
from cherry_etl.config import ChdbWriterConfig
from cherry_etl.writers.chdb import Writer


@pytest.fixture
def blocks():
    return pa.table(
        {
            "number": pa.array([10, 20, 30], type=pa.uint64()),
            "timestamp": pa.array([1, 2, 3], type=pa.uint64()),
        }
    )


@pytest.mark.asyncio
async def test_chdb_writer(tmp_path, blocks):
    db_path = tmp_path / "test.db"
    writer = Writer(ChdbWriterConfig(db_path=str(db_path), engine="MergeTree()"))
    table_name = "test_db.test_table"

    await writer.push_data({table_name: blocks})

    with closing(chdb.connect(str(db_path))) as conn:
        result = conn.query(f"SELECT * FROM {table_name}", "ArrowTable")
        assert result.cast(blocks.schema).equals(blocks)
