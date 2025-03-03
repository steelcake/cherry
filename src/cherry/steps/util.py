import pyarrow as pa


def record_batch_from_schema(schema: pa.Schema) -> pa.RecordBatch:
    arrays = []

    for t in schema.types:
        arrays.append(pa.array([], type=t))

    return pa.record_batch(
        arrays,
        schema=schema,
    )


def arrow_table_to_batch(table: pa.Table) -> pa.RecordBatch:
    batches = table.to_batches()
    return (
        pa.concat_batches(batches)
        if len(batches) > 0
        else record_batch_from_schema(table.schema)
    )
