import pyarrow as pa
from copy import deepcopy


def arrow_schema_binary_to_string(schema: pa.Schema) -> pa.Schema:
    schema = deepcopy(schema)

    for i, name in enumerate(schema.names):
        dt = schema.field(i).type
        if pa.types.is_binary(dt):
            dt = pa.utf8()
        schema = schema.set(i, pa.field(name, dt))

    return schema


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
