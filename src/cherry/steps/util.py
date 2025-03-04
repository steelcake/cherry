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


def arrow_table_to_batch(table: pa.Table) -> pa.RecordBatch:
    arrays = []
    for col in table.columns:
        arrays.append(col.combine_chunks())

    return pa.RecordBatch.from_arrays(arrays, names=table.column_names)
