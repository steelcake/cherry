import pyarrow as pa
from copy import deepcopy


def arrow_schema_cast_by_type(
    schema: pa.Schema, from_type: pa.DataType, to_type: pa.DataType
) -> pa.Schema:
    schema = deepcopy(schema)

    for i, name in enumerate(schema.names):
        dt = schema.field(i).type
        if dt == from_type:
            dt = to_type
        schema = schema.set(i, pa.field(name, dt))

    return schema


def arrow_schema_binary_to_string(schema: pa.Schema):
    return arrow_schema_cast_by_type(schema, pa.binary(), pa.string())


def arrow_table_to_batch(table: pa.Table) -> pa.RecordBatch:
    arrays = []
    for col in table.columns:
        arrays.append(col.combine_chunks())

    return pa.RecordBatch.from_arrays(arrays, names=table.column_names)
