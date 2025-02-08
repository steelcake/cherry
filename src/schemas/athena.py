from typing import Dict
import pyarrow as pa

ATHENA_TYPE_MAP = {
    'uint64': 'bigint',
    'int64': 'bigint',
    'uint32': 'int',
    'int32': 'int',
    'string': 'string',
    'bool': 'boolean',
    'timestamp[us]': 'timestamp',
    'double': 'double',
    'float': 'float'
}

def get_athena_schema(table: pa.Table) -> Dict[str, str]:
    """Convert PyArrow schema to Athena compatible types"""
    return {
        field.name: ATHENA_TYPE_MAP.get(str(field.type), 'string')
        for field in table.schema
    } 