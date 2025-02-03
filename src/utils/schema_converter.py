import polars as pl
import pyarrow as pa
from typing import Dict, List, Union
import logging

logger = logging.getLogger(__name__)

class SchemaConverter:
    """Converts between different schema formats"""
    
    @staticmethod
    def to_polars(arrow_schema: Union[pa.Schema, List[pa.Field]]) -> Dict[str, pl.DataType]:
        """Convert PyArrow schema to Polars schema"""
        if isinstance(arrow_schema, list):
            arrow_schema = pa.schema(arrow_schema)
            
        polars_schema = {}
        type_map = {
            pa.string(): pl.Utf8,
            pa.int64(): pl.Int64,
            pa.int32(): pl.Int32,
            pa.float64(): pl.Float64,
            pa.float32(): pl.Float32,
            pa.bool_(): pl.Boolean,
            pa.binary(): pl.Binary,
            pa.date32(): pl.Date,
            pa.timestamp('ns'): pl.Datetime,
        }

        for field in arrow_schema:
            try:
                if field.type in type_map:
                    polars_schema[field.name] = type_map[field.type]
                else:
                    # Default to string for unknown types
                    logger.warning(f"Unknown type {field.type} for field {field.name}, defaulting to string")
                    polars_schema[field.name] = pl.Utf8
            except Exception as e:
                logger.error(f"Error converting field {field.name}: {e}")
                raise

        return polars_schema

    @staticmethod
    def to_arrow(polars_schema: Dict[str, pl.DataType]) -> pa.Schema:
        """Convert Polars schema to PyArrow schema"""
        type_map = {
            pl.Utf8: pa.string(),
            pl.Int64: pa.int64(),
            pl.Int32: pa.int32(),
            pl.Float64: pa.float64(),
            pl.Float32: pa.float32(),
            pl.Boolean: pa.bool_(),
            pl.Binary: pa.binary(),
            pl.Date: pa.date32(),
            pl.Datetime: pa.timestamp('ns'),
        }

        fields = []
        for name, dtype in polars_schema.items():
            try:
                if dtype in type_map:
                    fields.append(pa.field(name, type_map[dtype]))
                else:
                    # Default to string for unknown types
                    logger.warning(f"Unknown type {dtype} for field {name}, defaulting to string")
                    fields.append(pa.field(name, pa.string()))
            except Exception as e:
                logger.error(f"Error converting field {name}: {e}")
                raise

        return pa.schema(fields) 